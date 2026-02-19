import os
import requests

class SocrataClient:
    """
    Cliente simple para consultar la API pública de Socrata.
    Maneja sesion HTTP y App Token opcional.
    """
    def __init__(self):
        self.domain = os.environ["SOCRATA_DOMAIN"]
        self.dataset = os.environ["SOCRATA_DATASET"]
        self.base = f"https://{self.domain}/resource/{self.dataset}.json"

        self.session = requests.Session()
        token = os.getenv("SOCRATA_APP_TOKEN", "").strip()
        if token:
            self.session.headers.update({"X-App-Token": token})

    def get(self, params: dict):
        r = self.session.get(self.base, params=params, timeout=90)
        r.raise_for_status()
        return r.json()
