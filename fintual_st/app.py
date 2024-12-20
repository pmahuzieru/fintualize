from fintual_api import FintualAPI

api = FintualAPI()
api.get_token()

print(api.get_asset_providers())
