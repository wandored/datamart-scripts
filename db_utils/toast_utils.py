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
    def __init__(self):
        self.api_access_url = Config.TOAST_API_ACCESS_URL
        self.access_token = self.generate_access_token()
        with DatabaseConnection() as self.db_connection:
            self.guid_list = self.fetch_locations(self.db_connection.cur)

        self.headers = {
            "Toast-Restaurant-External-ID": None, #Set dynamically for each request
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
        return self.access_token["accessToken"] if self.access_token else None
    
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
                        token = token.get("token")
                    if token and isinstance(token, str):
                        payload = self.decode_jwt(token)
                        exp = payload.get("exp", 0)
                        if time.time() < exp - 60:  # valid for more than 1 minute
                            return token
        except Exception as e:
            logging.warning(f"Error reading token cache: {e}")

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
                logging.info(
                    f"Failed to fetch access token. Response: {response.text}"
                )
                return None
        except requests.RequestException as e:
            logging.error(f"Error fetching access token: {e}")
            return None

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
      results = []
      page_token = None
      if(guid is None):
        raise ValueError("GUID is required for Toast API requests.")

      while True:
          request_params = params.copy() if params else {}
          if page_token:
              request_params["pageToken"] = page_token

          self.headers["Toast-Restaurant-External-ID"] = guid
          response = requests.get(self.api_access_url + url, headers=self.headers, params=request_params)

          if not response.ok:
              print(f"Error {response.status_code}: {response.text}")
              break

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

          time.sleep(rate_limit_wait)

      return results
    
    def extract_menu_items(self, guid, menu_id, menu_name, menu_group, parent_group_path=None):
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
    
    def get_restaurants(self, token):
      managementGroupGUID = Config.MANAGEMENT_GROUP_GUID
      url = (
          "/restaurants/v1/groups/"
          + managementGroupGUID
          + "/restaurants"
      )
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