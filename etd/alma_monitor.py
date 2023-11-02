import logging
from mongo_util import MongoUtil

class AlmaMonitor():

    def poll_for_alma_submissions(self, test_collection=None):
        query = {"alma_submission_status": "ALMA_DROPBOX"}
        logger = logging.getLogger('etd_alma_monitor')
        logger.info("Starting poll for alma submissions")
        mongo_util = MongoUtil()
        if test_collection  is not None:
            mongo_util.set_collection(mongo_util.db[test_collection])
        matching_records = mongo_util.query_records(query)
        logger.info(f"Found {len(matching_records)} matching records")
        record_list = list(matching_records)
        return record_list
    