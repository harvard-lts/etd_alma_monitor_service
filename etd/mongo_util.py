import pymongo
import os
import logging


class MongoUtil():   # pragma: no cover, not used by unit tests

    logger = logging.getLogger('etd_alma_monitor')
    status_field = "alma_submission_status"
    status_value = "ALMA_DROPBOX"

    def __init__(self):
        self.client = pymongo.MongoClient(os.getenv("MONGO_URL"))
        self.db = self.client[os.getenv("MONGO_DB")]
        self.collection = self.db[os.getenv("MONGO_TEST_COLLECTION")]

    def insert_record(self, record):  # pragma: no cover, not currently used
        self.collection.insert_one(record)

    def insert_records(self, records):
        self.collection.insert_many(records)

    def query_records(self, query={}, fields=None):
        if fields is not None:
            return list(self.collection.find(query, fields))
        else:  # pragma: no cover, not currently used by app
            return list(self.collection.find(query))

    def delete_records(self):
        self.collection.delete_many({})

    def close_connection(self):
        self.client.close()

    def __del__(self):
        self.close_connection()

    # we use this if we want to set to the test collection for testing
    def set_collection(self, collection):
        self.collection = collection
