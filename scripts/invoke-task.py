from celery import Celery
import os
from datetime import datetime

now = datetime.now()
timestamp = now.strftime("%Y%m%d%H%M")
job_ticket_id = f"{timestamp}_etd_alma_monitor_invoke_task"

app1 = Celery('tasks')
app1.config_from_object('celeryconfig')

arguments = {"job_ticket_id": job_ticket_id, "feature_flags": {
            'dash_feature_flag': os.getenv("DASH_FEATURE_FLAG"),
            'alma_feature_flag': os.getenv("ALMA_FEATURE_FLAG"),
            'send_to_drs_feature_flag':
            os.getenv("SEND_TO_DRS_FEATURE_FLAG")}}

res = app1.send_task('etd-alma-monitor-service.tasks.send_to_drs',
                     args=[arguments], kwargs={},
                     queue=os.getenv("CONSUME_QUEUE_NAME"))
