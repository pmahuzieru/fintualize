from fintual_st.db.db import DatabaseManager


def get_asset_provider_ids(db_manager: DatabaseManager):
    query = """
        SELECT DISTINCT id FROM asset_provider;
    """

    try:
        db_manager.connect()
        result_df = db_manager.execute_query(query=query)
        return result_df["id"].to_list()
    finally:
        db_manager.disconnect()


if __name__ == '__main__':
    db_manager = DatabaseManager()
    print(get_asset_provider_ids(db_manager))
