'''
    This Lambda function is triggered when a file lands in s3 bucket coursera-degrees-data (DataHub)
    via a CloudWatch trigger
    Once the event is parsed, it is looked up with an s3 file and the MEGA API is called to trigger a Job
    Full Documentation:
        https://docs.google.com/document/d/1cuI2KNlzr6mOwjjjrJ5txtNFfyOHgP_ET-QWl4F9tWY/edit#
    deployed to:
        https://console.aws.amazon.com/lambda/home?region=us-east-1#/functions/datahub_job_runner
    For this function to run successfly:
        - A lambda layer has been created: https://console.aws.amazon.com/lambda/home?region=us-east-1#/layers/lambda_job_runner_layer/versions/3
        - With requirements file: https://github.com/webedx-spark/di-services/tree/master/lambda/datahub/datahub_job_runner
        - To add a lambda layer: https://docs.aws.amazon.com/lambda/latest/dg/configuration-layers.html
        - Specifically for this lambda:
            1. Install docker application
            2. Create a folder on your local machine add the following:
                1. requirements.txt
                2. folder call it "python"
                3. file call it  "bash.sh"
            3. go to the terminal switch to the folder
            4. execute the command "bash build.sh"
            5. zip the python file
            6. add the python.zip file to the lambda layer and select python 3.7 under compartible runtime.
            7. go to the datahub_job_runner and select layers and under select the layer you created.
'''

import json
import re
import boto3
import csv
import requests
import logging

# from datadog import datadog_lambda_wrapper, lambda_metric

s3 = boto3.client('s3')


class MegaJob:
    def __init__(self, jobId, user='datahub@coursera.org'):
        self.base_url = 'http://envoy-gateway.prod.dkandu.me:30003'
        MEGA_JOB_LATEST_TEMPLATE = self.base_url + \
            '/api/megaJobManager.v1?action=getLatest&id={job_id}'
        self.http_headers = {
            'X-Coursera-Source': 'Internal',
            'X-Coursera-Destination-Service': 'mega'
        }
        self.jobId = jobId
        self.user = user
        self.latest_id = requests.post(MEGA_JOB_LATEST_TEMPLATE.format(job_id=self.jobId),
                                       headers=self.http_headers,
                                       json={}).json()['id']
        # print(self.latest_id)

    def execute_mega_job(self):
        MEGA_JOB_EXECUTE_TEMPLATE = self.base_url + \
            '/api/megaJobManager.v1?action=execute&id={versioned_job_id}&requesterEmailOverride={user}'

        requests.post(MEGA_JOB_EXECUTE_TEMPLATE.format(
            versioned_job_id=self.latest_id, user=self.user),
            headers=self.http_headers, json={}
        )


def number_of_records_in_s3_file(bucket, key):
    i = 0
    try:
        for _ in s3.get_object(Bucket=bucket, Key=key)['Body'].read().splitlines(True):
            if i == 2:
                return i
            i += 1
        return i
    except BaseException:
        # This is important in case a file cannot be read e.g. when its held up by other resources
        return 2


def log(metric_name, metric_type='count', metric_value=1, tags=[]):
    '''
    DataDog log format for future integration:
    https://docs.datadoghq.com/integrations/amazon_lambda/
    MONITORING|unix_epoch_timestamp|metric_value|metric_type|my.metric.name|#tag1:value,tag2
    print("MONITORING|{}|{}|{}|{}|#{}".format(
        int(time.time()), metric_value, metric_type, 'hasher.lambda.' + metric_name, ','.join(tags)
    ))
    '''
    # Print JSON for Insights to parse and query
    log_dict = {}
    log_dict['metric'] = metric_name
    for tag in tags:
        k = tag.split(':')[0]
        v = tag.split(':')[1]
        log_dict[k] = v
    print(json.dumps(log_dict))

# @datadog_lambda_wrapper


def lambda_handler(event, context):
    filename = '(none)'
    partner = '(none)'
    jobId = ""
    jobUrl = ""
    try:
        event_key = event['detail']['requestParameters']['key']
        filename = event_key.split('/')[-1]
        partner = event_key.split('/')[0]
    except BaseException:
        # the key cannot be parsed from the event object
        log(metric_name='event_failure')

    try:
        # export of s3 jobs to s3: https://tools.coursera.org/mega/s3Export/SUy04I2YEemSJm-AiuSdSA~S3_EXPORT_JOB
        # query to get s3 jobs: https://tools.coursera.org/mega/latestquery/0ZjJII2XEemSJm-AiuSdSA
        res = s3.get_object(Bucket='oaljadda', Key='jobs/s3_import_jobs.csv')
        lines = res['Body'].read().decode('utf-8').splitlines(True)
        reader = csv.DictReader(lines)
        keys = {}
        for line in reader:
            keys[line['s3_key']] = [line['mega_job_id'], line['script_url']]
        # key from the event has the date, eg 20190613, need to use placeholder
        event_key_template = re.sub(r'\d{8}', '{DATE_PLACEHOLDER}', event_key)

        if(event_key_template in keys):
            jobId = keys.get(event_key_template)[0]
            jobUrl = keys.get(event_key_template)[1]
            line_count = number_of_records_in_s3_file(
                'coursera-degrees-data', event_key)
            # Need at least 2 rows of data to proceed(header, plus data)
            if line_count < 2:
                log(metric_name='not_enough_records', tags=[
                    'file:{}'.format(filename), 'partner:{}'.format(partner)])
                return False
            else:
                job = MegaJob(jobId)
                job.execute_mega_job()
                print('running job: {}'.format(jobUrl))
                log(metric_name='run_mega_job', tags=['file:{}'.format(
                    filename), 'partner:{}'.format(partner), 'job:{}'.format(jobId)])

    except BaseException as e:
        logging.exception(e)
        metric_name = 'Datahub Runner Job Failure'
        tags = ['file:{}'.format(filename), 'partner:{}'.format(partner)]
        log(metric_name=metric_name, tags=tags)
        send_email(metric_name, tags, jobUrl)
    return None

# Send Alert Email


def send_email(metric_name, tags, jobId):
    '''
    This method sends an email

    '''
    fromEmail = "datahub@coursera.org"
    replyTo = "datahub@coursera.org"

    subject = metric_name
    client = boto3.client('ses')
    toEmail = ["data-engineering-team@coursera.org",
               "sam@coursera.org", "mfitzmaurice@coursera.org"]
    message = "\n\n" + metric_name + ": For " + \
        tags[1] + " " + tags[0] + " " + jobId
    response = client.send_email(
        Source=fromEmail,
        Destination={
            'ToAddresses': toEmail,
        },
        Message={
            'Subject': {
                'Data': subject,
                'Charset': 'utf8'
            },
            'Body': {
                'Text': {
                    'Data': message,
                    'Charset': 'utf8'
                }
            }
        },
        ReplyToAddresses=[
            replyTo
        ]
    )
    # Print Message in Cloud watch.
    print(response)
    print(message)
    return None
