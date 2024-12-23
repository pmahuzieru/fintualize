import os
from urllib.parse import urljoin
import requests

from fintual_st.helpers import read_credentials

FINTUAL_API_ROOT = 'https://fintual.cl/api/'


class FintualAPI:

    def __init__(self):
        self.root_url = FINTUAL_API_ROOT
        self.user_email = None
        self.auth_token = None

    def get_token(self):
        endpoint = urljoin(FINTUAL_API_ROOT, "access_tokens")
        if not self.auth_token:
            credentials = read_credentials()

            self.user_email = credentials.get("email")

            body = {"user": credentials}
            response = requests.post(endpoint, json=body)

            if response.status_code == 201:
                response_data = response.json()["data"]
                token = response_data["attributes"]["token"]
                self.auth_token = token
                print("Got token succesfully")
            else:
                print(f"Problem fetching token: Status Code {response.status_code} {response.reason}")

    def get_goals(self):
        endpoint = urljoin(FINTUAL_API_ROOT, "goals")

        if not self.auth_token:
            print("Get auth token first.")
            return
        
        params = {
            "user_email": self.user_email,
            "user_token": self.auth_token
        }

        response = requests.get(endpoint, params=params)

        print(response.json())

    def get_asset_providers(self):
        endpoint = urljoin(FINTUAL_API_ROOT, "asset_providers")

        response = requests.get(endpoint)

        if response.status_code != 200:
            print(f"Error getting asset providers: {response.status_code} {response.reason}")
            return
        
        return response.json()
    
    def get_conceptual_assets(self, asset_provider_id: int):
        subpaths = [FINTUAL_API_ROOT] + ['asset_providers', f'{asset_provider_id}', 'conceptual_assets']
        endpoint = "/".join(subpaths)
        response = requests.get(endpoint)

        if response.status_code != 200:
            print(f"Error getting conceptual assets: {response.status_code} {response.reason}")
            return
        
        return response.json()
        


    



