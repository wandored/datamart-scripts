import base64
import json
import logging
import os
import time

import pandas as pd
import requests

from db_utils.config import Config
from db_utils.dbconnect import DatabaseConnection


class ToastClient:
    def __init__(self, load_locations=True):
        self.api_access_url = Config.TOAST_API_ACCESS_URL
        self.access_token = self.generate_access_token()
        # New source updaters select their own explicitly scoped locations.
        # Keep the legacy selection available to existing callers.
        self.guid_list = pd.DataFrame(columns=["id", "name", "toast_guid"])
        if load_locations:
            with DatabaseConnection() as self.db_connection:
                self.guid_list = self.fetch_locations(self.db_connection.cur)

        self.headers = {
            "Toast-Restaurant-External-ID": None,  # Set dynamically for each request
            "Authorization": f"Bearer {self.get_access_token()}",
        }

    # Helper Functions
    def decode_jwt(self, token):
        """Decode a JWT without verification just to extract the payload"""
        payload = token.split(".")[1]
        padded = payload + "=" * (-len(payload) % 4)  # JWT base64 padding
        decoded_bytes = base64.urlsafe_b64decode(padded)
        return json.loads(decoded_bytes)

    def fetch_locations(self, cur) -> pd.DataFrame:
        cur.execute(
            """
            SELECT id, name, toast_guid
            FROM restaurants
            WHERE email IS NOT Null
            ORDER BY name
            """
        )
        locations = cur.fetchall()

        return pd.DataFrame(locations, columns=["id", "name", "toast_guid"])

    # Return Current Access Token
    def get_access_token(self):
        if isinstance(self.access_token, dict):
            return self.access_token.get("accessToken")
        return self.access_token

    def get_api_access_url(self):
        return self.api_access_url

    def get_locations(self):
        return self.guid_list

    # Generate Access Token (Was get_access_token)
    def generate_access_token(self):
        """
        Fetches the OAuth2 access token required to authenticate API requests.
        """
        try:
            if os.path.exists(Config.TOKEN_CACHE_FILE):
                with open(Config.TOKEN_CACHE_FILE) as f:
                    cache = json.load(f)
                    token = cache.get("token")
                    if isinstance(token, dict):
                        token = token.get("accessToken") or token.get("token")
                    if token and isinstance(token, str):
                        payload = self.decode_jwt(token)
                        exp = payload.get("exp", 0)
                        if time.time() < exp - 60:  # valid for more than 1 minute
                            return token
        except Exception:
            logging.warning("Unable to read Toast token cache; requesting a new token")

        url = self.api_access_url + "/authentication/v1/authentication/login"
        headers = {
            "Content-Type": "application/json"  # This ensures the correct content type
        }
        data = {
            "userAccessType": Config.USER_ACCESS_TYPE,
            "clientId": Config.CLIENT_ID,
            "clientSecret": Config.CLIENT_SECRET,
        }

        try:
            response = requests.post(url, headers=headers, json=data, timeout=15)
            response.raise_for_status()
            token = response.json().get("token")

            if token:
                with open(Config.TOKEN_CACHE_FILE, "w") as f:
                    json.dump({"token": token}, f)
                logging.info("Fetched new access token")
                return token
            else:
                logging.error("Toast authentication returned no token")
                return None
        except requests.RequestException:
            logging.error("Toast authentication request failed")
            return None

    def request(self, endpoint, guid, params=None):
        """Authenticated GET with a timeout and bounded transient-error retries.

        Return the response so pagination callers can inspect its headers. Errors
        never include response bodies or authentication headers in their message.
        """
        if guid is None:
            raise ValueError("GUID is required for Toast API requests")
        token = self.get_access_token()
        if not token:
            raise RuntimeError("Toast authentication did not return an access token")
        if not endpoint.startswith("/") or endpoint.startswith("//"):
            raise ValueError("Toast endpoint must be a relative API path")
        headers = {
            "Toast-Restaurant-External-ID": str(guid),
            "Authorization": f"Bearer {token}",
        }
        for attempt in range(3):
            try:
                response = requests.get(
                    self.api_access_url.rstrip("/") + endpoint,
                    headers=headers,
                    params=params,
                    timeout=60,
                    allow_redirects=False,
                )
            except (requests.Timeout, requests.ConnectionError):
                if attempt == 2:
                    raise requests.RequestException(
                        f"Toast GET {endpoint} failed for restaurant {guid} after 3 attempts"
                    ) from None
            else:
                if 200 <= response.status_code < 300:
                    return response
                retryable = response.status_code in (429, 500, 502, 503, 504)
                if not retryable or attempt == 2:
                    raise requests.HTTPError(
                        f"Toast GET {endpoint} for restaurant {guid}: HTTP {response.status_code}",
                        response=response,
                    )
                # Do not retry earlier than a long/unsupported Retry-After value.
                retry_after = response.headers.get("Retry-After")
                if retry_after is not None:
                    try:
                        delay = float(retry_after)
                    except ValueError:
                        delay = float("inf")
                    if not 0 <= delay <= 60:
                        raise requests.HTTPError(
                            f"Toast restaurant {guid}: HTTP {response.status_code}; retry later",
                            response=response,
                        ) from None
                    time.sleep(delay)
                    continue
            time.sleep(2 ** attempt)

    def get_restaurant(self, guid):
        """Return a single RestaurantInfo object, including archived locations."""
        return self.request(
            f"/restaurants/v1/restaurants/{guid}", guid,
            params={"includeArchived": "true"},
        ).json()

    def get_response_data(self, url, guid, params=None, rate_limit_wait=1.0):
        """
        Fetch all pages from a paginated Toast API endpoint.

        Args:
            url (str): The base URL of the Toast API endpoint.
            headers (dict): Headers to include in the request (must include authorization).
            params (dict, optional): Any initial query parameters. Can include 'startDate', 'endDate', etc.
            rate_limit_wait (float): Time (in seconds) to wait between paginated requests, to avoid rate limits.

        Returns:
            List[dict]: Aggregated list of results from all pages.
        """
        page = 1

        results = []
        page_token = None
        if guid is None:
            raise ValueError("GUID is required for Toast API requests.")

        while True:
            request_params = params.copy() if params else {}
            if page_token:
                request_params["pageToken"] = page_token

            response = self.request(url, guid, params=request_params)

            # Add current page of data to results
            data = response.json()
            if isinstance(data, list):
                results.extend(data)
            elif isinstance(data, dict):
                # Some endpoints wrap results under a key (e.g., 'discounts', 'orders')
                # Add your key if needed
                for key in data:
                    if isinstance(data[key], list):
                        results.extend(data[key])
                        break
                else:
                    results.append(data)

            # Get next page token
            page_token = response.headers.get("toast-next-page-token")

            if not page_token:
                break

            page += 1

            time.sleep(rate_limit_wait)

        return results

    def get_paged_response_data(
        self,
        url,
        guid,
        params=None,
        page_size=100,
        rate_limit_wait=1.0,
        parse_float=None,
    ):
        """
        Fetch all pages from a Toast endpoint that uses page-number pagination.

        Args:
            url (str): Endpoint path.
            guid (str): Restaurant GUID.
            params (dict, optional): Initial query parameters.
            page_size (int): Number of records per page (max 100).
            rate_limit_wait (float): Delay between requests.
            parse_float: Optional JSON number decoder (e.g. Decimal for source prices).

        Returns:
            list: Aggregated results from all pages.
        """
        if guid is None:
            raise ValueError("GUID is required for Toast API requests.")
        if type(page_size) is not int or not 1 <= page_size <= 100:
            raise ValueError("page_size must be an integer from 1 through 100")

        results = []
        page = 1

        while True:
            request_params = params.copy() if params else {}
            request_params["page"] = page
            request_params["pageSize"] = page_size

            response = self.request(url, guid, params=request_params)

            data = response.json(parse_float=parse_float) if parse_float else response.json()

            if isinstance(data, list):
                results.extend(data)
                records = len(data)
            else:
                raise ValueError(f"Expected list response, got {type(data).__name__}")

            # No next page if fewer than page_size records returned.
            if records < page_size:
                break

            # A full page may have a continuation even without a Link header.
            # Request until a short/empty page so a missing header cannot truncate data.
            page += 1
            time.sleep(rate_limit_wait)

        return results

    def extract_menu_items(
        self, guid, menu_id, menu_name, menu_group, parent_group_path=None
    ):
        extracted = []

        group_name = menu_group.get("name", "")
        group_path = (
            f"{parent_group_path} > {group_name}" if parent_group_path else group_name
        )

        # Add items if they exist at this level
        for item in menu_group.get("menuItems", []):
            extracted.append(
                {
                    "location_guid": guid,
                    "menu_id": menu_id,
                    "menu_name": menu_name,
                    "menu_group_name": group_path,
                    "toast_item_name": item.get("name", ""),
                    "pos_name": item.get("posName", ""),
                    "kitchen_name": item.get("kitchenName", ""),
                }
            )

        # Recursively explore nested subgroups
        for subgroup in menu_group.get("menuGroups", []):
            extracted.extend(
                self.extract_menu_items(guid, menu_id, menu_name, subgroup, group_path)
            )

        # print(
        #     f"{'  ' * group_path.count('>')}Group: {group_name} — Items: {len(menu_group.get('menuItems', []))}"
        # )
        return extracted

    def get_restaurants(self):
        managementGroupGUID = Config.MANAGEMENT_GROUP_GUID
        url = "/restaurants/v1/groups/" + managementGroupGUID + "/restaurants"
        data = self.get_response_data(url, Config.TOAST_RESTAURANT_EXTERNAL_ID)
        guid_list = [item["guid"] for item in data]
        drop_list = Config.LOCATION_DROP_LIST
        guid_list = [item for item in guid_list if item not in drop_list]
        return guid_list

    def get_restaurant_config(self, token, guid_list):
        df = pd.DataFrame()
        for guid in guid_list:
            url = self.api_access_url + "/restaurants/v1/restaurants/" + guid
            headers = {
                "Toast-Restaurant-External-ID": guid,
                "Authorization": f"Bearer {token}",
            }
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                json_data = response.json()
                general = json_data.get("general", {})
                data = {
                    "location_guid": guid,
                    "concept": general.get("name", ""),
                    "location_name": general.get("locationName", ""),
                    "location_code": general.get("locationCode", ""),
                }
                extracted_data = pd.DataFrame([data])
                df = pd.concat([df, extracted_data])
            else:
                raise RuntimeError(
                    f"Failed to fetch restaurant config for GUID {guid}: {response.status_code} - {response.text}"
                )
        return df
