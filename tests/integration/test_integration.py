import pymongo
import datetime
import os
from etd.worker import Worker
from etd.alma_monitor import AlmaMonitor
from etd.mongo_util import MongoUtil


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

class TestMongoIntegrationClass():

    records = [
        {
            "proquest_id": 1234567,
            "school_alma_dropbox": "gsd",
            "alma_submission_status": "ALMA_DROPBOX",
            "insertion_date": datetime.datetime.now().isoformat(),
            "last_modified_date": datetime.datetime.now().isoformat(),
            "alma_dropbox_submission_date":
            datetime.datetime.now().isoformat()
        },
        {
            "proquest_id": 2345678,
            "school_alma_dropbox": "dce",
            "alma_submission_status": "ALMA_DROPBOX",
            "insertion_date": datetime.datetime.now().isoformat(),
            "last_modified_date": datetime.datetime.now().isoformat(),
            "alma_dropbox_submission_date":
            datetime.datetime.now().isoformat()
        },
        {
            "proquest_id": 3456789,
            "school_alma_dropbox": "college",
            "alma_submission_status": "ALMA",
            "insertion_date": datetime.datetime.now().isoformat(),
            "last_modified_date": datetime.datetime.now().isoformat(),
            "alma_dropbox_submission_date":
            datetime.datetime.now().isoformat()
        }             
    ]

    def test_poll_for_alma_submissions(self):
        # setup the test collection with 3 records
        self.__setup_test_collection()
        # call the poller
        alma_monitor = AlmaMonitor()
        records = alma_monitor.poll_for_alma_submissions()

        assert len(records) == 2

        # teardown the test collection
        self.__teardown_test_collection()

    def __setup_test_collection(self):
        mongo_util = MongoUtil()
        mongo_util.set_collection(mongo_util.db[os.getenv("MONGO_TEST_COLLECTION")])
        mongo_util.insert_records(TestMongoIntegrationClass.records)
        mongo_util.close_connection()

    def __teardown_test_collection(self):
        mongo_util = MongoUtil()
        mongo_util.set_collection(mongo_util.db[os.getenv("MONGO_TEST_COLLECTION")])
        mongo_util.delete_records()
        mongo_util.close_connection()
        

