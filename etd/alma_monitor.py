import logging
from mongo_util import MongoUtil
from . import configure_logger
import os
import requests
import xml.etree.ElementTree as ET
from opentelemetry import trace
from opentelemetry.trace import Status
from opentelemetry.trace import StatusCode
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import SERVICE_NAME

# tracing setup
JAEGER_NAME = os.getenv('JAEGER_NAME')
JAEGER_SERVICE_NAME = os.getenv('JAEGER_SERVICE_NAME')
resource = Resource(attributes={SERVICE_NAME: JAEGER_SERVICE_NAME})
provider = TracerProvider(resource=resource)
otlp_exporter = OTLPSpanExporter(endpoint=JAEGER_NAME, insecure=True)
span_processor = BatchSpanProcessor(otlp_exporter)
provider.add_span_processor(span_processor)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)


class AlmaMonitor():

    def __init__(self, test_collection=None):
        configure_logger()
        self.mongoutil = MongoUtil()
        if test_collection is not None:  # pragma: no cover, only changes collection # noqa
            self.mongoutil.set_collection(self.mongoutil.db[test_collection])
            

    logger = logging.getLogger('etd_alma_monitor')

    @tracer.start_as_current_span(
            "invoke_alma_monitor_monitor_alma_and_invoke_dims")
    def monitor_alma_and_invoke_dims(self):
        record_list = self.poll_for_alma_submissions()
        for record in record_list:
            current_span = trace.get_current_span()
            current_span.add_event("checking Alma for pqid {}" \
                                   .format(record[MongoUtil.FIELD_PQ_ID]))
            try:
                if self.check_alma(record[MongoUtil.FIELD_PQ_ID]):
                    current_span.add_event("{} found in Alma" \
                                    .format(record[MongoUtil.FIELD_PQ_ID]))
                    self.mongoutil.update_status(record[MongoUtil.FIELD_PQ_ID],
                                                 MongoUtil.ALMA_STATUS)
                    self.invoke_dims(record)
            except Exception as e:
                self.logger.error(f"Error querying records: {e}")
                current_span.set_status(Status(StatusCode.ERROR))
                current_span.add_event("error checking Alma for pqid")
                current_span.record_exception(e)
                self.mongoutil.close_connection()
                raise e
            
        self.mongoutil.close_connection()

    @tracer.start_as_current_span(
            "invoke_alma_monitor_poll_for_alma_submissions")
    def poll_for_alma_submissions(self):
            """
            Polls MongoDB for records with alma_submission_status set to
            "ALMA_DROPBOX".
            Returns a list of matching records.

            Args:
                test_collection (str, optional): Name of the collection
                to use for testing. Defaults to None.

            Returns:
                list: List of matching records.
            """
            current_span = trace.get_current_span()
            current_span.add_event("polling mongo for pqids in dropbox")
            query = {MongoUtil.FIELD_SUBMISSION_STATUS: MongoUtil.ALMA_DROPBOX_STATUS}
            fields = {MongoUtil.FIELD_PQ_ID: 1, MongoUtil.FIELD_SCHOOL_ALMA_DROPBOX: 1,
                      MongoUtil.FIELD_SUBMISSION_STATUS: 1, MongoUtil.FIELD_DIRECTORY_ID: 1}
            self.logger.info("Starting poll for alma submissions")
            matching_records = []
            
            try:
                matching_records = self.mongoutil.query_records(query, fields)
                self.logger.info(f"Found {len(matching_records)} matching records")
                self.logger.debug(f"Matching records: {matching_records}")
            except Exception as e:
                self.logger.error(f"Error querying records: {e}")
                current_span.set_status(Status(StatusCode.ERROR))
                current_span.add_event("Unable to query mongo for records")
                current_span.record_exception(e)
                raise e
            record_list = list(matching_records)
            self.logger.debug(f"Record list: {record_list}")
            return record_list
    
    def check_alma(self, proquest_id):
        alma_sru_base = os.getenv("ALMA_SRU_BASE") + proquest_id
        response = requests.get(alma_sru_base)
        num_records = self.get_number_alma_records(response.content)
        if num_records == 1:
            return True
        elif num_records == 0:
            return False
        else:
            raise Exception(f"Unexpected number of records: {num_records}")
    
    def get_number_alma_records(self, xml_response):
        root = ET.fromstring(xml_response)
        file = root.find('numberOfRecords')
        if file is not None:
            return int(file.text)
        return 0

# {
#      “package_id”: //string //directory_id
#      “fs_path”: //string  -> Mongo record (directory_id) and combo of /etd dropbox
#      “filename”: //string
#      “s3_path”: //empty but required
#      “s3_bucket_name”: //empty string (required)
#      “admin_metadata”:
#           {
#              	"depositingSystem": "ETD",
#               "ownerCode": "HUL.TEST",
#               "billingCode": "HUL.TEST.BILL_0001",
#               "urnAuthorityPath": "HUL.GUEST",
# 		 “dash_id”: “12345”,  //mets file
# 		 “alma_id”: “12345”, //recordInfo/recordIdentifier
# 		 “pq_id”: “12345”, // Mongo record
# 		 “embargo_date”: //Date if exists in mets.xml,
# 		 “file_info”: { //Comma-delim objects for each file
# 			<original_file_name>: {
# 			 //Modified to have ONLY alphanumeric,
# 			 // periods, underscores, dashes
# 			 “modified_file_name”: //string 
# 			 “object_role”: //Role of the object
# 			 “file_role”: //Role of the file
# 			 “object_osn”: //formatted osn
# 			 “file_osn”: //formatted osn
#          }
# }

    def invoke_dims(self, record):
        # Verify the submission exists in the directory

        # Use the mets extractor

        # Use schools.py to pull owner code, billing code,
        # and urnAuthorityPath

        # Build the object and file OSNs

        # Determine file roles (?) this might already be done in DTS

        # Create the json object
        pass
