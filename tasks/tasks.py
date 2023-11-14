from celery import Celery
from celery import bootsteps
from celery.signals import worker_ready
from celery.signals import worker_shutdown
from pathlib import Path
import os
import logging
import etd
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import SERVICE_NAME
from opentelemetry.trace.propagation.tracecontext \
    import TraceContextTextMapPropagator
import json
from etd.alma_monitor import AlmaMonitor
from etd.mongo_util import MongoUtil

app = Celery()
app.config_from_object('celeryconfig')
etd.configure_logger()
logger = logging.getLogger('etd_alma_monitor')

FEATURE_FLAGS = "feature_flags"
SEND_TO_DRS_FEATURE_FLAG = "send_to_drs_feature_flag"
FIELD_SUBMISSION_STATUS = "alma_submission_status"
FIELD_PQ_ID = "proquest_id"
FIELD_SCHOOL_ALMA_DROPBOX = "school_alma_dropbox"
FIELD_DIRECTORY_ID = "directory_id"
ALMA_DROPBOX_STATUS = "ALMA_DROPBOX"
ALMA_STATUS = "ALMA"

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

# heartbeat setup
# code is from
# https://github.com/celery/celery/issues/4079#issuecomment-1270085680
hbeat_path = os.getenv("HEARTBEAT_FILE", "/tmp/worker_heartbeat")
ready_path = os.getenv("READINESS_FILE", "/tmp/worker_ready")
update_interval = float(os.getenv("HEALTHCHECK_UPDATE_INTERVAL", 15.0))
HEARTBEAT_FILE = Path(hbeat_path)
READINESS_FILE = Path(ready_path)
UPDATE_INTERVAL = update_interval  # touch file every 15 seconds


class LivenessProbe(bootsteps.StartStopStep):
    requires = {'celery.worker.components:Timer'}

    def __init__(self, worker, **kwargs):  # pragma: no cover
        self.requests = []
        self.tref = None

    def start(self, worker):  # pragma: no cover
        self.tref = worker.timer.call_repeatedly(
            UPDATE_INTERVAL, self.update_heartbeat_file,
            (worker,), priority=10,
        )

    def stop(self, worker):  # pragma: no cover
        HEARTBEAT_FILE.unlink(missing_ok=True)

    def update_heartbeat_file(self, worker):  # pragma: no cover
        HEARTBEAT_FILE.touch()


@worker_ready.connect
def worker_ready(**_):  # pragma: no cover
    READINESS_FILE.touch()


@worker_shutdown.connect
def worker_shutdown(**_):  # pragma: no cover
    READINESS_FILE.unlink(missing_ok=True)


app.steps["worker"].add(LivenessProbe)


@app.task(serializer='json', name='etd-alma-monitor-service.tasks.send_to_drs')
def monitor_alma_and_invoke_dims(json_message):
    # Here we need to add these steps, with methods in teh etd/alma_monitor.py
    # 1. call etd/alma_monitor.poll_for_alma_submissions
    # 2. Check if records are in Alma (call etd/alma_monitor.check_alma)
    # 3. If records are in Alma, call etd/alma_monitor.update_alma_status
    # 4. Invoke DIMS (call etd/alma_monitor.invoke_dims)

    ctx = None
    if "traceparent" in json_message:  # pragma: no cover, tracing is not being tested # noqa: E501
        carrier = {"traceparent": json_message["traceparent"]}
        ctx = TraceContextTextMapPropagator().extract(carrier)
    with tracer.start_as_current_span("send_to_drs", context=ctx) \
            as current_span:

        if FEATURE_FLAGS in json_message:
            feature_flags = json_message[FEATURE_FLAGS]
            if SEND_TO_DRS_FEATURE_FLAG in feature_flags and \
                    feature_flags[SEND_TO_DRS_FEATURE_FLAG] == "on":  # pragma: no cover, unit test should not create a DRS record # noqa: E501
                logger.debug("FEATURE IS ON>>>>>SEND TO DRS")
                current_span.add_event("FEATURE IS ON>>>>>SEND TO DRS")
                # Get the list of records that have been submitted to Alma
                collection = None
                if "integration_test" in json_message:
                    collection = os.getenv("MONGO_TEST_COLLECTION")
                alma_monitor = AlmaMonitor(collection)
                mongoutil = MongoUtil()
                submitted_records = alma_monitor.poll_for_alma_submissions()
                logger.debug("Alma submitted records:")
                logger.debug(submitted_records)
                for record in submitted_records:
                    logger.debug("Record: {}".format(record))
                    if "integration_test" in json_message:
                        alma_id = os.getenv("INTEGRATION_TEST_ALMA_ID")
                        record["integration_test"] = True
                    else:
                        alma_id = alma_monitor.get_alma_id(
                            record[FIELD_PQ_ID])
                    if alma_id is not None:
                        current_span.add_event("{} found in Alma".format(
                            record[FIELD_PQ_ID]))
                        mongoutil.update_status(
                            record[FIELD_PQ_ID],
                            ALMA_DROPBOX_STATUS,
                            ALMA_STATUS)
                        if FIELD_DIRECTORY_ID not in record:
                            logger.error("Directory ID not found in record {}".format(record[FIELD_PQ_ID])) # noqa
                            current_span.add_event("NO DIR ID FOUND for {}".format(record[FIELD_PQ_ID])) # noqa
                        else:
                            # Send to DRS
                            alma_monitor.invoke_dims(record, alma_id)
            else:
                # Feature is off so do hello world
                return invoke_hello_world(json_message)
        else:
            # No feature flags so do hello world for now
            return invoke_hello_world(json_message)


# To be removed when real logic takes its place
@tracer.start_as_current_span("invoke_hello_world_alma_monitor")
def invoke_hello_world(json_message):

    # For 'hello world', we are also going to place a
    # message onto the etd_ingested_into_drs queue
    # to allow the pipeline to continue
    current_span = trace.get_current_span()
    new_message = {"hello": "from etd-alma-monitor-service"}
    if 'identifier' in json_message:
        proquest_identifier = json_message['identifier']
        new_message["identifier"] = proquest_identifier
        current_span.set_attribute("identifier", proquest_identifier)
        logger.debug("processing id: " + str(proquest_identifier))
    if FEATURE_FLAGS in json_message:
        logger.debug("FEATURE FLAGS FOUND")
        logger.debug(json_message[FEATURE_FLAGS])
        new_message[FEATURE_FLAGS] = json_message[FEATURE_FLAGS]
        current_span.add_event("FEATURE FLAGS FOUND")
        current_span.add_event(json.dumps(json_message[FEATURE_FLAGS]))

    # If only unit testing, return the message and
    # do not trigger the next task.
    if "unit_test" in json_message:
        return new_message
    carrier = {}  # pragma: no cover, tracing is not being tested # noqa: E501
    TraceContextTextMapPropagator().inject(carrier)  # pragma: no cover, tracing is not being tested # noqa: E501
    traceparent = carrier["traceparent"]  # pragma: no cover, tracing is not being tested # noqa: E501
    new_message["traceparent"] = traceparent  # pragma: no cover, tracing is not being tested # noqa: E501
    current_span.add_event("to next queue")  # pragma: no cover, tracing is not being tested # noqa: E501
    app.send_task("etd-alma-drs-holding-service.tasks.add_holdings",
                  args=[new_message], kwargs={},
                  queue=os.getenv('PUBLISH_QUEUE_NAME'))  # pragma: no cover, does not reach this for unit testing # noqa: E501
    return {}  # pragma: no cover, does not reach this for unit testing # noqa: E501
