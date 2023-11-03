import logging
from mongo_util import MongoUtil
import os
from . import configure_logger

class AlmaMonitor():

    def __init__(self):
        configure_logger()
            
    logger = logging.getLogger('etd_alma_monitor')

    def poll_for_alma_submissions(self, test_collection=None):

        query = {"alma_submission_status": "ALMA_DROPBOX"}
        fields = {"proquest_id": 1, "school_alma_dropbox": 1, "alma_submission_status": 1}
        self.logger.info("Starting poll for alma submissions")
        mongo_util = MongoUtil()
        matching_records = []
        if test_collection  is not None:
            mongo_util.set_collection(mongo_util.db[test_collection])
        try:    
            matching_records = mongo_util.query_records(query, fields)
            self.logger.info(f"Found {len(matching_records)} matching records")
            self.logger.debug(f"Matching records: {matching_records}")
        except Exception as e:
            self.logger.error(f"Error querying records: {e}")
        record_list = list(matching_records)
        self.logger.debug(f"Record list: {record_list}")
        mongo_util.close_connection()
        return record_list
