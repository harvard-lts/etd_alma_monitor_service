import pymongo
import datetime
import os
from etd.worker import Worker


class TestWorkerIntegrationClass():

    def test_api(self):
        expected_msg = "REST api is running."
        worker = Worker()
        msg = worker.call_api()
        assert msg == expected_msg

    def test_api_fail(self):
        expected_msg = "REST api is NOT running."
        worker = Worker()
        msg = worker.call_api()
        assert msg != expected_msg

    def test_get_pending_pqids(self):
        # Create a MongoDB client and connect to your MongoDB server
        client = pymongo.MongoClient(os.getenv("MONGO_URL"))

        # Select a database (you can create it if it doesn't exist)
        db = client[os.getenv("MONGO_DB")]
        collection = db[os.getenv("MONGO_TEST_COLLECTION")]
        document = {
            "proquest_id": 12345678,
            "school_alma_dropbox": "gsd",
            "alma_submission_status": "ALMA_DROPBOX",
            "insertion_date": datetime.datetime.now().isoformat(),
            "last_modified_date": datetime.datetime.now().isoformat(),
            "alma_dropbox_submission_date":
            datetime.datetime.now().isoformat()
        }
        collection.insert_one(document)
        query = {"alma_submission_status": "ALMA_DROPBOX"}
        matching_documents = collection.find(query)
        # for doc in matching_documents:
        #    print(doc)
        document_list = list(matching_documents)
        # print(document_list)
        # print(len(document_list))
        assert len(document_list) == 1
        collection.delete_many({})
        client.close()
