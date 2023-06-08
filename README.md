# ETD Alma Monitor Service
A service that notifies the DAIS system when an ETD is ready to be ingested into the DRS

<img src="https://github.com/harvard-lts/etd_alma_monitor_service/actions/workflows/pytest.yml/badge.svg">

<img src="https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/ives1227/4c52af43b2bf4d034ff3b4f8938a8f93/raw/covbadge.json">

# Using this Repository
Read the wiki in the etd-base-template repo for instructions on how to set up the template and badge for your repository:
https://github.com/harvard-lts/etd-base-template/wiki

### References

- Coverage badge adapted from [Ned Batchelder](https://nedbatchelder.com/blog/202209/making_a_coverage_badge.html)

### Run hello world example locally

- Clone this repo from github 
- Create the .env by copying the .example.env
`cp .env.example .env`
- Replace rabbit connect value with dev values (found in 1Password LTS-ETD)
- Replace the `CONSUME_QUEUE_NAME` with a unique name for local testing (eg - add your initials to the end of the queue names)
- Start up docker  
`docker-compose -f docker-compose-local.yml up --build -d --force-recreate`

- Bring up [DEV ETD Rabbit UI](https://b-7ecc68cb-6f33-40d6-8c57-0fbc0b84fa8c.mq.us-east-1.amazonaws.com/)
- Look for `CONSUME_QUEUE_NAME` queue

- Exec into the docker container
`docker exec -it etd-alma-monitor-service bash`
- Run invoke task python script
`python3 scripts/invoke-task.py`

- View the `etd_ingested_into_drs` queue for a message from the monitor service.  Please note - this is a temporary published message to work with the hello world pipeline.


### Manually placing a message on the queue

- Open the queue in the RabbitMQ UI
- Click on the `CONSUME_QUEUE_NAME` queue (the name that you assigned this env value to)
- Open Publish Message
- Set a property of `content_type` to `application/json`
- Set the Payload to the following JSON content
`{"id": "da28b429-e006-49a5-ae77-da41b925bd85","task": 'etd-alma-monitor-service.tasks.invoke_dims","args": [{"hello":"world"}]}`
