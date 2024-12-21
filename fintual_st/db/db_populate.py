import pandas as pd
from fintual_st.db.db import DatabaseManager
from fintual_st.db.queries import get_asset_provider_ids
from fintual_st.fintual_api import FintualAPI

api = FintualAPI()
db_manager = DatabaseManager()

def insert_conceptual_assets():
    """
    TODO: it's giving too many requests error, must try to call once to db, and store asset
    provider id in the db as well.
    """
    
    asset_provider_ids = get_asset_provider_ids(db_manager=db_manager)

    for asset_provider_id in asset_provider_ids:

        conceptual_assets_response = api.get_conceptual_assets(asset_provider_id)

        if conceptual_assets_response:
            conceptual_assets_df = _conceptual_assets_to_df(conceptual_assets_response)
            if len(conceptual_assets_df) > 0:
                _batch_insert_dataframe(conceptual_assets_df, table_name="conceptual_assets")
                print(f"Values inserted into 'conceptual_assets'! (provider {asset_provider_id})")
    
def _conceptual_assets_to_df(conceptual_assets_response):
    df_data = []
    for conceptual_asset in conceptual_assets_response.get("data", []):
        attributes = conceptual_asset["attributes"]
        conceptual_asset_data = {
            "id": conceptual_asset["id"],
            "name": attributes.get("name"),
            "symbol": attributes.get("symbol"),
            "category": attributes.get("category"),
            "currency": attributes.get("currency"),
            "max_scale": attributes.get("max_scale"),
            "run": attributes.get("run"),
            "data_source": attributes.get("data_source")
        }
        df_data.append(conceptual_asset_data)
    return pd.DataFrame(df_data)


def insert_asset_providers():
    asset_providers_df = _asset_providers_to_df(api.get_asset_providers())

    if asset_providers_df is not None:
        _batch_insert_dataframe(asset_providers_df, table_name="asset_provider")
        print("Values inserted into 'asset_providers'!")

def _asset_providers_to_df(asset_providers_response) -> pd.DataFrame | None:
    if asset_providers_response:
        asset_providers_data = asset_providers_response.get("data", [])
        df_data = []
        for asset_provider in asset_providers_data:
            row_data = {
                "id": asset_provider["id"],
                "name": asset_provider["attributes"]["name"]
            }
            df_data.append(row_data)
        return pd.DataFrame(df_data)
    print("Error turning asset providers into df: response empty")

def _batch_insert_dataframe(dataframe: pd.DataFrame, table_name: str):
    # Prepare INSERT query
    columns = ", ".join(dataframe.columns)
    placeholders = ", ".join("?" for _ in dataframe.columns)

    query = f"""
        INSERT OR IGNORE INTO {table_name} ({columns})
        VALUES ({placeholders})
    """

    records = dataframe.to_records(index=False)
    records_list = [tuple(record) for record in records]

    # Execute query
    try:
        db_manager.connect()
        db_manager.execute_query(query=query, parameters=records_list, executemany=True)
    finally:
        db_manager.disconnect()

if __name__ == '__main__':
    insert_conceptual_assets()
