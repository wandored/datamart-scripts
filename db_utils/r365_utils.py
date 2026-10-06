import requests
from urllib.parse import urljoin, urlsplit
from db_utils.config import Config


class R365ODataClient:
    """Read OData pages without returning partial results on request failure."""

    def __init__(self):
        self.base_url = Config.SRVC_ROOT.rstrip("/") + "/"
        self.session = requests.Session()
        self.session.auth = (Config.SRVC_USER, Config.SRVC_PSWRD)
        self.session.headers["Accept"] = "application/json"

    def get_all(self, entity, params=None):
        url = urljoin(self.base_url, entity)
        visited = set()
        while url:
            # Continuation URLs must not receive credentials on another host.
            if urlsplit(url).netloc != urlsplit(self.base_url).netloc or (
                urlsplit(url).scheme != urlsplit(self.base_url).scheme
            ):
                raise ValueError("Unexpected OData continuation origin")
            if url in visited:
                raise ValueError("Repeated OData continuation link")
            visited.add(url)
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or not isinstance(payload.get("value"), list):
                raise ValueError("OData response requires a value array")
            yield from payload["value"]
            next_link = payload.get("@odata.nextLink")
            if next_link is not None and not isinstance(next_link, str):
                raise ValueError("Invalid OData continuation link")
            url = urljoin(response.url, next_link) if next_link else None
            params = None


class R365Client:
    def __init__(self):
        self.base_url = Config.R365_BASE_URL.rstrip("/")
        self.session = requests.Session()

        self.session.headers.update(
            {
                "Authorization": f"Bearer {Config.R365_TOKEN}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "X-R365-context-security-id": Config.R365_SECURITY_ID,
                "X-R365-context-tenant-id": Config.R365_TENANT_ID,
            }
        )

    def request(self, method, endpoint, params=None, json=None):

        if endpoint.startswith("http"):
            url = endpoint
        else:
            base = urlsplit(self.base_url)
            if base.path and endpoint.startswith(f"{base.path}/"):
                # Continuation links can already include the /public base path.
                url = f"{base.scheme}://{base.netloc}{endpoint}"
            else:
                url = f"{self.base_url}{endpoint}"

        response = self.session.request(
            method=method,
            url=url,
            params=params,
            json=json,
            timeout=60,
        )

        response.raise_for_status()

        if response.content:
            return response.json()

        return None

    def get_all(
        self, endpoint, params=None, collection_key="items", next_link_key="nextLink"
    ):

        response = self.request(
            "GET",
            endpoint,
            params=params,
        )

        while response:
            records = response.get(collection_key, [])

            if isinstance(records, list):
                yield from records
            elif records is not None:
                yield records

            next_link = response.get(next_link_key)

            if not next_link:
                break

            response = self.request(
                "GET",
                next_link,
            )

    def get_resource(
        self, domain, resource, collection_key="items", next_link_key="nextLink", **params
    ):
        endpoint = f"/v1/{domain}/{resource}"
        return list(
            self.get_all(
                endpoint, params=params, collection_key=collection_key,
                next_link_key=next_link_key,
            )
        )
