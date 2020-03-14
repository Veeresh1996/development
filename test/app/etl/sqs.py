
# system imports
import json

# lib imports
import boto3

# local imports
from .log import Log
from .opts import Opts

class SQS(object):
    def __init__(self, log: Log, opts: Opts):
        super().__init__()
        self.log = log.new('sqs')
        self.job_id = opts.job_id
        self.sqs_queue_url = opts.sqs_queue_url
        self.sqs = None
        if self.sqs_queue_url:
            self.sqs = boto3.client('sqs', endpoint_url=opts.sqs_endpoint_url)

    def send_status(self, data: dict):
        sqsmsg = {
            "Status": data['status'].value,
            "JobId": self.job_id,
            "AttemptedInputS3Uri": data['input_path']
        }
        if "message" in data:
            sqsmsg['Message'] = data['message']
        if "output_paths" in data:
            sqsmsg['Outputs'] = data['output_paths']
        self.log.info("SQS Message: '{}'".format(json.dumps(sqsmsg)))
        if self.sqs:
            response = self.sqs.send_message(
                QueueUrl=self.sqs_queue_url,
                MessageBody=json.dumps(sqsmsg)
            )
        else:
            self.log.warn("SQS not configured!")
