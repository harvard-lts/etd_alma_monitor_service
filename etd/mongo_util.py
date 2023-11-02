import pymongo
import os
import logging

class MongoUtil():

    logger = logging.getLogger('etd_alma_monitor')
    status_field = "alma_submission_status"
    status_value = "ALMA_DROPBOX"

    def __init__(self):
        self.client = pymongo.MongoClient(os.getenv("MONGO_URL"))
        self.db = self.client[os.getenv("MONGO_DB")]
        self.collection = self.db[os.getenv("MONGO_TEST_COLLECTION")]

    def insert_record(self, record):
        self.collection.insert_one(record)

    def insert_records(self, records):
        self.collection.insert_many(records)

    def query_records(self, query={}, fields=None):
        if fields is not None:
            return list(self.collection.find(query, fields))
        else:
            return list(self.collection.find(query))

    def delete_records(self):
        result = self.collection.delete_many({})

    def close_connection(self):
        self.client.close()

    # we use this if we want to set to the test collection for testing
    def set_collection(self, collection):
        self.collection = collection

    def __del__(self):
        self.close_connection()

    def query_by_alma_status(collection):
        __connect_to_mongodb(self)
        query = {status_field: status_value}
        matching_records = collection.find(query)
        record_list = list(matching_records)
        client.close()
        return record_list

    def __connect_to_mongodb(self):
        client = pymongo.MongoClient(os.getenv("MONGO_URL"))
        db = client[os.getenv("MONGO_DB")]
        collection = db[os.getenv("MONGO_TEST_COLLECTION")]
        return collection
    