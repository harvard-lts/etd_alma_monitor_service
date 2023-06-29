from celery import Celery
import os
import requests
etd.configure_logger()
logger = logging.getLogger('etd_alma_monitor')

app = Celery()
app.config_from_object('celeryconfig')


@app.task(serializer='json', name='etd-alma-monitor-service.tasks.send_to_drs')
def invoke_dims(message):
    logger.info("message")
    logger.info(message)
    dims_ingest_url = os.getenv("DIMS_INGEST_URL")
    # Temporarily using a get call since we are testing
    # with a healtcheck endpoint for 'hello world'
    r = requests.get(dims_ingest_url, verify=False)
    print(r.text)
    invoke_hello_world()


# To be removed when real logic takes its place
def invoke_hello_world():

    # For 'hello world', we are also going to place a
    # message onto the etd_ingested_into_drs queue
    # to allow the pipeline to continue
    new_message = {"hello": "from etd-alma-monitor-service"}
    app.send_task("etd-alma-drs-holding-service.tasks.add_holdings",
                  args=[new_message], kwargs={},
                  queue="etd_ingested_into_drs")
