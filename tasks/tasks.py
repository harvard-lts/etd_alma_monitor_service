from celery import Celery
import os
import requests
import logging
import etd

app = Celery()
app.config_from_object('celeryconfig')
etd.configure_logger()
logger = logging.getLogger('etd_alma_monitor')

FEATURE_FLAGS = "feature_flags"
SEND_TO_DRS_FEATURE_FLAG = "send_to_drs_feature_flag"


@app.task(serializer='json', name='etd-alma-monitor-service.tasks.send_to_drs')
def invoke_dims(json_message):
    logger.debug("message")
    logger.debug(json_message)
    dims_ingest_url = os.getenv("DIMS_INGEST_URL")
    if dims_ingest_url is not None:
        # Temporarily using a get call since we are testing
        # with a healtcheck endpoint for 'hello world'
        r = requests.get(dims_ingest_url, verify=False)
        logger.debug(r.text)
    if FEATURE_FLAGS in json_message:
        feature_flags = json_message[FEATURE_FLAGS]
        if SEND_TO_DRS_FEATURE_FLAG in feature_flags and \
                feature_flags[SEND_TO_DRS_FEATURE_FLAG] == "on":
            # Send to DRS
            logger.debug("FEATURE IS ON>>>>>SEND TO DRS")
        else:
            # Feature is off so do hello world
            return invoke_hello_world(json_message)


# To be removed when real logic takes its place
def invoke_hello_world(json_message):

    # For 'hello world', we are also going to place a
    # message onto the etd_ingested_into_drs queue
    # to allow the pipeline to continue
    new_message = {"hello": "from etd-alma-monitor-service"}
    if FEATURE_FLAGS in json_message:
        logger.debug("FEATURE FLAGS FOUND")
        logger.debug(json_message[FEATURE_FLAGS])
        new_message[FEATURE_FLAGS] = json_message[FEATURE_FLAGS]

    # If only unit testing, return the message and
    # do not trigger the next task.
    if "unit_test" in json_message:
        return new_message

    app.send_task("etd-alma-drs-holding-service.tasks.add_holdings",
                  args=[new_message], kwargs={},
                  queue="etd_ingested_into_drs_dd")
    return {}
