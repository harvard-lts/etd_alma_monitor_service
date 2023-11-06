import logging
from mongo_util import MongoUtil
from . import configure_logger
import os
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

    def __init__(self):
        configure_logger()

    logger = logging.getLogger('etd_alma_monitor')

    @tracer.start_as_current_span(
            "invoke_alma_monitor_poll_for_alma_submissions")
    def poll_for_alma_submissions(self, test_collection=None):
        current_span = trace.get_current_span()
        current_span.add_event("polling mongo for pqids in dropbox")
        query = {"alma_submission_status": "ALMA_DROPBOX"}
        fields = {"proquest_id": 1, "school_alma_dropbox": 1,
                  "alma_submission_status": 1}
        self.logger.info("Starting poll for alma submissions")
        mongo_util = MongoUtil()
        matching_records = []
        if test_collection is not None:  # pragma: no cover, only changes collection # noqa
            mongo_util.set_collection(mongo_util.db[test_collection])
        try:
            matching_records = mongo_util.query_records(query, fields)
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
        mongo_util.close_connection()
        return record_list
