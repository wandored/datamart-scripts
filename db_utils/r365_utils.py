import requests
from db_utils.config import Config


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

    def get_all(self, endpoint, params=None):

        response = self.request(
            "GET",
            endpoint,
            params=params,
        )

        while response:
            yield from response.get("items", [])

            next_link = response.get("nextLink")

            if not next_link:
                break

            response = self.request(
                "GET",
                next_link,
            )

    def get_resource(self, domain, resource, **params):
        endpoint = f"/v1/{domain}/{resource}"
        return list(self.get_all(endpoint, params=params))
