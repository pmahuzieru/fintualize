import pandas as pd
from fintual_st.db.db import DatabaseManager
from fintual_st.fintual_api import FintualAPI

api = FintualAPI()
db_manager = DatabaseManager()

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
    insert_asset_providers()
