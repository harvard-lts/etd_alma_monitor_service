from celery import Celery
import os
import requests
import logging
import etd
import json

app = Celery()
app.config_from_object('celeryconfig')
etd.configure_logger()
logger = logging.getLogger('etd_alma_monitor')


@app.task(serializer='json', name='etd-alma-monitor-service.tasks.send_to_drs')
def invoke_dims(message):
    logger.info("message")
    logger.info(message)
    dims_ingest_url = os.getenv("DIMS_INGEST_URL")
    # Temporarily using a get call since we are testing
    # with a healtcheck endpoint for 'hello world'
    r = requests.get(dims_ingest_url, verify=False)
    print(r.text)
    json_message = json.loads(message)
    if "feature_flags" in json_message:
        feature_flags = json_message["feature_flags"]
        if "send_to_drs_feature_flag" in feature_flags and \
                feature_flags["send_to_drs_feature_flag"] == "on":
            # Send to DRS
            print("FEATURE IS ON>>>>>SEND TO DRS")
        else:
            # Feature is off so do hello world
            invoke_hello_world(json_message)


# To be removed when real logic takes its place
def invoke_hello_world(json_message):

    # For 'hello world', we are also going to place a
    # message onto the etd_ingested_into_drs queue
    # to allow the pipeline to continue
    new_message = {"hello": "from etd-alma-monitor-service"}
    if "feature_flags" in json_message:
        print("FEATURE FLAGS FOUND")
        print(json_message['feature_flags'])
        new_message['feature_flags'] = json_message['feature_flags']
    app.send_task("etd-alma-drs-holding-service.tasks.add_holdings",
                  args=[new_message], kwargs={},
                  queue="etd_ingested_into_drs_dd")
