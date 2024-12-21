from fintual_st.fintual_api import FintualAPI

api = FintualAPI()
api.get_token()

print(api.get_conceptual_assets(60))
