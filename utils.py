from datetime import datetime, timedelta

import boto3

def stream_name(arn):
    try:
        return arn.split("/")[-1]
    except e:
        return 'INVALID ARN AT NAME EXTRACTION: ' + arn


def start_stream_aws(s):
    client = boto3.client(
        'kinesis',
        aws_access_key_id=s["access_key"],
        aws_secret_access_key=s["secret_key"],
        region_name='eu-west-1'
    )

    name = stream_name(s['arn'])

    client.create_stream(
        StreamName=name,
        ShardCount=1
    )


def stop_stream_aws(s):
    client = boto3.client(
        'kinesis',
        aws_access_key_id=s["access_key"],
        aws_secret_access_key=s["secret_key"],
        region_name='eu-west-1'
    )

    name = stream_name(s['arn'])

    client.delete_stream(
        StreamName=name
    )


def row_dict(row):
    return dict(zip(row.keys(), row))


def new_expiry_time():
    return datetime.now() + timedelta(hours=7.5)
    #return datetime.now() + timedelta(seconds=5)


def encode_arn(arn):
    return arn.replace("/","--SLASH--")


def decode_arn(enc_arn):
    return enc_arn.replace("--SLASH--", "/")


def get_feeder_config_str(s):
    return """{{
  "cloudwatch.emitMetrics": true,
  "kinesis.endpoint": "https://kinesis.eu-west-1.amazonaws.com",
  "firehose.endpoint": "https://firehose.eu-west-1.amazonaws.com",
  "awsAccessKeyId":"{}",
  "awsSecretAccessKey":"{}",
  "flows": [
    {{
      "filePattern": "/tmp/transactions.log*",
      "kinesisStream": "{}",
      "partitionKeyOption": "RANDOM",
      "maxBufferAgeMillis": 1000,
      "maxBufferSizeRecords": 500,
      "initialPosition": "END_OF_FILE"
    }}
  ]
}}
""".format(s["access_key"], s["secret_key"], stream_name(s["arn"]))
