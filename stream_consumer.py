import requests
import json
import base64


def thingsboard(records):
    access_token = 'Put your Device Access Token here'
    for r in records:
        data = r['kinesis']['data']
        decoded_data = base64.b64decode(data).decode('iso-8859-1')
        requests.post(
            'https://thingsboard.cloud/api/v1/' + access_token + '/telemetry',
            data=decoded_data
        )


def main(event, context):
    thingsboard(event['Records'])
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
