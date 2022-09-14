from mtDB.db.MTDB import MTDB

if __name__ == "__main__":
    database = MTDB("/home/pranav/MTCacheData/output_dump")
    print(database._get_combined_df())