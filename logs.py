import boto3
import time
import sys


baseGroupName = 'arc-ng.mtvnservices.com/production/'
serviceName = 'casl-migration'

if (len(sys.argv) > 1):
    serviceName = sys.argv[1]

groupName = 'arc-ng.mtvnservices.com/production/' + serviceName
print('---- logs for' + groupName)

session = boto3.Session(profile_name='saml')
logsClient = session.client('logs')


def get_last_stream_name():
    response = logsClient.describe_log_streams(
        logGroupName=groupName,
        orderBy='LastEventTime',
        descending=True,
        limit=10
    )

    # print(response['nextToken'])
    # print(response['ResponseMetadata'])
    return response['logStreams'][0]['logStreamName']


def get_log_events(stream_name, **kwargs):
    basic_args = {
        'logGroupName': groupName,
        'logStreamName': stream_name,
        'startFromHead': False
    }
    not_none_kwargs = {k:v for k,v in kwargs.items() if v is not None}
    args = {**basic_args, **not_none_kwargs}
    return logsClient.get_log_events(**args)


print('--------------------------------------')
lastStreamName = get_last_stream_name()
print(lastStreamName)
nextToken = None
limit = 10

while True:
    if nextToken is not None:
        limit = None
    logEvents = get_log_events(lastStreamName, nextToken=nextToken, limit=limit)
    nextToken = logEvents['nextForwardToken']
    events = logEvents['events']

    for event in events:
        print(event['message'])

    time.sleep(5)
