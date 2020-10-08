import pymongo
import pandas as pd


def get_mongo_db():
    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
    return mongo_client["ais"]


def get_collection_df(collection: str, mongo_db) -> pd.DataFrame:
    try:
        collection_obj = mongo_db[collection]
        collection_cursor = collection_obj.find({})
        return pd.DataFrame(list(collection_cursor)).drop('_id', 1)
    # What error would be thrown
    except Exception as e:
        print(e)


# Needs proper docstrings
# Write simple query for each collection in MongoDB and print to screen
if __name__ == '__main__':
    # get strings from conf
    db = get_mongo_db()

    total_rows_df = get_collection_df("total_rows", db)

    duplicates_count_df = get_collection_df("duplicates_count", db)

    most_common_value_df = get_collection_df("most_common_value", db)

    nulls_count_df = get_collection_df("nulls_count", db)

    unique_values_df = get_collection_df("unique_values", db)

    print(total_rows_df)
    print(duplicates_count_df)
    print(most_common_value_df)
    print(nulls_count_df)
    print(unique_values_df)
