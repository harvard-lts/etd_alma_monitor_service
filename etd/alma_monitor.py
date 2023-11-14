import logging
from etd.mongo_util import MongoUtil
import etd.mongo_util as mongo_util
from . import configure_logger
import os
import requests
import xml.etree.ElementTree as ET
from etd.mets_extractor import MetsExtractor
import zipfile
import etd.schools as schools
import shutil
import re
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

AMD_PRIMARY = "amd_primary"
AMD_SUP = "amd_supplemental"
AMD_LIC = "amd_license"
ROLE_THESIS = "THESIS"
ROLE_SUPPLEMENT = "THESIS_SUPPLEMENT"
ROLE_LICENSE = "LICENSE"
ROLE_DOCUMENTATION = "DOCUMENTATION"
ROLE_ARCHIVAL_MASTER = "ARCHIVAL_MASTER"

namespaces = {'xb': 'http://www.loc.gov/zing/srw/'}


class AlmaMonitor():

    def __init__(self, test_collection=None):
        configure_logger()
        self.mongoutil = MongoUtil()
        if test_collection is not None:  # pragma: no cover, only changes collection # noqa
            self.mongoutil.set_collection(self.mongoutil.db[test_collection])

    logger = logging.getLogger('etd_alma_monitor')

    @tracer.start_as_current_span(
            "invoke_alma_monitor_monitor_alma_and_invoke_dims")
    def monitor_alma_and_invoke_dims(self): # pragma: no cover, covered in integration testing # noqa
        record_list = self.poll_for_alma_submissions()
        submitted_records = []
        for record in record_list:
            current_span = trace.get_current_span()
            current_span.add_event("checking Alma for pqid {}"
                                   .format(record[mongo_util.FIELD_PQ_ID]))
            try:
                alma_id = self.get_alma_id(record[mongo_util.FIELD_PQ_ID])
                if alma_id is not None:
                    submitted_records.append(alma_id)
                    current_span.add_event("{} found in Alma".format(
                        record[mongo_util.FIELD_PQ_ID]))
                    self.mongoutil.update_status(
                        record[mongo_util.FIELD_PQ_ID], mongo_util.ALMA_STATUS)
                    self.invoke_dims(record, alma_id)
            except Exception as e:
                self.logger.error(f"Error querying records: {e}")
                current_span.set_status(Status(StatusCode.ERROR))
                current_span.add_event("error checking Alma for pqid")
                current_span.record_exception(e)
                self.mongoutil.close_connection()
                raise e

        self.mongoutil.close_connection()
        return submitted_records

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
        query = {mongo_util.FIELD_SUBMISSION_STATUS:
                 mongo_util.ALMA_DROPBOX_STATUS}
        fields = {mongo_util.FIELD_PQ_ID: 1,
                  mongo_util.FIELD_SCHOOL_ALMA_DROPBOX: 1,
                  mongo_util.FIELD_SUBMISSION_STATUS: 1,
                  mongo_util.FIELD_DIRECTORY_ID: 1}
        self.logger.info("Starting poll for alma submissions")
        matching_records = []

        try:
            matching_records = self.mongoutil.query_records(query, fields)
            self.logger.info("Found {len(matching_records)} \
                             matching records")
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

    def get_alma_id(self, proquest_id): # pragma: no cover, covered in integration testing # noqa
        alma_sru_base = os.getenv("ALMA_SRU_BASE") + proquest_id
        response = requests.get(alma_sru_base)
        num_records = self.get_number_alma_records(response.content)
        if num_records == 1:
            return self.get_alma_record_id(response.content)
        elif num_records == 0:
            return None
        else:
            raise Exception(f"Unexpected number of records: {num_records}")

    def get_number_alma_records(self, xml_response):
        root = ET.fromstring(xml_response)
        num_records_entry = root.find("xb:numberOfRecords", namespaces)
        if num_records_entry is not None:
            return int(num_records_entry.text)
        return 0

    def get_alma_record_id(self, xml_response):
        root = ET.fromstring(xml_response)
        record_id_entry = root.find(
            ".//xb:records/xb:record/xb:recordIdentifier", namespaces)
        if record_id_entry is not None:
            return record_id_entry.text
        return None

    def invoke_dims(self, record, alma_id): # pragma: no cover, covered in integration testing # noqa
        # Verify the submission exists in the directory
        data_dir = os.getenv("ALMA_DATA_DIR")
        submission_dir = os.path.join(data_dir,
                                      record[mongo_util.FIELD_DIRECTORY_ID])
        if not os.path.isdir(submission_dir):
            raise Exception("Submission directory \
                            {} not found".format(submission_dir))

        extractd_dir = self.__unzip_submission(submission_dir)
        # Get the zip file and unzip it
        if extractd_dir is None:
            raise Exception("Error unzipping submission \
                            {}".format(submission_dir))

        mets_file = os.path.join(extractd_dir, "mets.xml")
        if not os.path.isfile(mets_file):
            raise Exception(f"mets.xml not found in {extractd_dir}")

        # Use the mets extractor
        mets_extractor = MetsExtractor(mets_file)

        # Build the json DIMS data
        dims_json = self.__build_base_json_dims_data(
            mets_extractor, alma_id, record, data_dir)
        file_info_json = self.__build_file_info_json_data(
            extractd_dir, record, mets_extractor)
        dims_json["admin_metadata"]["file_info"] = file_info_json
        self.logger.debug("DIMS json: {}".format(dims_json))

        # Delete the extracted directory
        shutil.rmtree(extractd_dir)
        if "unit_testing" not in record: # pragma: no cover, don't call dims for unit testing # noqa
            # Call the DIMS API
            self.__call_dims_api(dims_json)

        return dims_json

    def __call_dims_api(self, payload_data): # pragma: no cover, no calling dims for testing # noqa
        dims_endpoint = os.getenv('DIMS_INGEST_URL')
        self.logger.debug("DIMS endpoint: {}".format(dims_endpoint))
        # Call DIMS ingest
        ingest_etd_export = None

        ingest_etd_export = requests.post(
            dims_endpoint,
            json=payload_data,
            verify=False)

        json_ingest_response = ingest_etd_export.json()
        if json_ingest_response["status"] == "failure":
            raise Exception("DIMS Ingest call failed")

    def __unzip_submission(self, submission_dir): # pragma: no cover, covered in integration testing # noqa
        for file in os.listdir(submission_dir):
            file_path = os.path.join(submission_dir, file)
            self.logger.debug("submission dir file: {}".format(file))
            if os.path.isfile(file_path) and file.endswith(".zip"):
                extracted_dir = os.path.join(submission_dir, "extracted")
                os.makedirs(extracted_dir, exist_ok=True)
                self.logger.debug("extracted dir: {}".format(extracted_dir))
                # unzip the file
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(extracted_dir)
                return extracted_dir
        return None

    def format_etd_osn(self, school_dropbox_name, filename,
                       pq_id, amdid, degree_date):
        '''Formats the OSN to use the format of 
        ETD_[OBJECT_ROLE]_[SCHOOL_CODE]_[DEGREE_DATE_VALUE]_PQ_[PROQUEST_IDENTIFIER_VALUE]''' # noqa
        if amdid is None and os.path.basename(filename) != "mets.xml":
            raise Exception("amdid is missing from mets for {}"
                            .format(filename), None)

        role = self.__determine_role(amdid, filename)

        osn = "ETD_{}_{}_{}_PQ_{}".format(role, school_dropbox_name,
                                          degree_date, pq_id)
        if role is None:
            osn = "ETD_{}_{}_PQ_{}".format(school_dropbox_name,
                                           degree_date, pq_id)
        return osn

    def __determine_role(self, amdid, filename):
        # mets.xml is the only one not in the mets.xml fileSec
        # so it will not have the amdid
        if amdid is None and os.path.basename(filename) == "mets.xml":
            return ROLE_DOCUMENTATION

        if amdid.startswith(AMD_PRIMARY):
            return ROLE_THESIS
        elif amdid.startswith(AMD_SUP):
            return ROLE_SUPPLEMENT
        elif amdid.startswith(AMD_LIC):
            return ROLE_LICENSE
        return None

    def __build_base_json_dims_data(self, mets_extractor, alma_id,
                                    record, data_dir): # pragma: no cover, covered in integration testing # noqa
        # Use schools.py to pull owner code, billing code,
        # and urnAuthorityPath
        owner_code = schools.school_info[
            record[mongo_util.FIELD_SCHOOL_ALMA_DROPBOX]]['owner_code']
        billing_code = schools.school_info[
            record[mongo_util.FIELD_SCHOOL_ALMA_DROPBOX]]['billing_code']
        urn_authority_path = schools.school_info[
            record[mongo_util.FIELD_SCHOOL_ALMA_DROPBOX]]['urn_authority_path']
        submission_dir = os.path.join(data_dir,
                                      record[mongo_util.FIELD_DIRECTORY_ID])
        zip_file = None
        for file in os.listdir(submission_dir):
            file_path = os.path.join(submission_dir, file)
            self.logger.debug("submission dir file: {}".format(file))
            if os.path.isfile(file_path) and file.endswith(".zip"):
                zip_file = file_path
                break
        if zip_file is None:
            raise Exception("No zip file found in {}".format(submission_dir))
        fs_source_path = os.path.join(submission_dir, zip_file)
        # Create the json object
        dims_json = {
                "package_id": record[mongo_util.FIELD_DIRECTORY_ID],
                "fs_source_path": fs_source_path,
                "s3_path": "",
                "s3_bucket_name": "",
                "depositing_application": "ETD"
        }
        admin_md_json = {
                "depositingSystem": "ETD",
                "ownerCode": owner_code,
                "billingCode": billing_code,
                "urnAuthorityPath": urn_authority_path,
                "dash_id": mets_extractor.get_dash_id(),
                "pq_id": "PQ-"+record[mongo_util.FIELD_PQ_ID],
                "alma_id": alma_id,
                # Required fields but no longer used
                "original_queue": "test",
                'task_name': "test",
                "retry_count": 0
        }
        dims_json["admin_metadata"] = admin_md_json
        return dims_json

    def __build_file_info_json_data(self, extractd_dir,
                                    record, mets_extractor): # pragma: no cover, covered in integration testing # noqa
        osn_tracker = {}
        file_info_json = {}
        for file in os.listdir(extractd_dir):
            modified_file_name = re.sub(r"[^\w\d\.\-]", "_", file)
            file_path = os.path.join(extractd_dir, file)
            self.logger.debug("file path: {}".format(file_path))
            # Build the object and file OSNs
            object_osn = self.format_etd_osn(
                record[mongo_util.FIELD_SCHOOL_ALMA_DROPBOX],
                file_path,
                record[mongo_util.FIELD_PQ_ID],
                mets_extractor.get_amdid_and_mimetype(file_path).amdid,
                mets_extractor.get_degree_date())

            # If the object OSN already exists, increment the sequence
            if object_osn in osn_tracker:
                sequence = osn_tracker[object_osn]
                sequence += 1
                osn_tracker[object_osn] = sequence
                object_osn = object_osn + "_" + str(sequence)
            else:
                osn_tracker[object_osn] = 1

            file_osn = object_osn + "_1"
            file_info_json[file] = {
                "modified_file_name": modified_file_name,
                "object_osn": object_osn,
                "file_osn": file_osn
            }

        return file_info_json
