import json
import boto3
import base64
from decimal import Decimal


def inserir_item(event):
    dynamodb = boto3.resource('dynamodb')
    tabela = dynamodb.Table('records')
    try:
        for record in event['Records']:
            data = record['kinesis']['data']
            decoded_data = base64.b64decode(data).decode('utf-8')
            with tabela.batch_writer() as batch_writer:
                batch_writer.put_item(
                    Item=json.loads(decoded_data, parse_float=Decimal)
                )
    except Exception as e:
        print(e)


def main(event, context):
    inserir_item(event)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
