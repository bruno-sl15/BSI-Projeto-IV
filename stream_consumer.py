from ast import literal_eval
import requests
import json
import base64


def thingsboard(records):
    token_recife = 'Put your Device Access Token here'
    token_petrolina = 'Put your Device Access Token here'
    token_arco_verde = 'Put your Device Access Token here'
    token_garanhus = 'Put your Device Access Token here'
    token_surubim = 'Put your Device Access Token here'
    token_cabrobo = 'Put your Device Access Token here'
    token_caruaru = 'Put your Device Access Token here'
    token_ibimirim = 'Put your Device Access Token here'
    token_serra_talhada = 'Put your Device Access Token here'
    token_floresta = 'Put your Device Access Token here'
    token_palmares = 'Put your Device Access Token here'
    token_ouricuri = 'Put your Device Access Token here'
    token_salgueiro = 'Put your Device Access Token here'
    for r in records:
        data = r['kinesis']['data']
        decoded_data = base64.b64decode(data).decode('iso-8859-1')
        decoded_data = decoded_data.replace('null', 'None')
        cidade = literal_eval(decoded_data)['CIDADE']
        if cidade == 'RECIFE':
            requests.post(
                'https://thingsboard.cloud/api/v1/' + token_recife + '/telemetry',
                data=decoded_data
            )
        elif cidade == 'PETROLINA':
            requests.post(
                'https://thingsboard.cloud/api/v1/' + token_petrolina + '/telemetry',
                data=decoded_data
            )
        elif cidade == 'ARCO VERDE':
            requests.post(
                'https://thingsboard.cloud/api/v1/' + token_arco_verde + '/telemetry',
                data=decoded_data
            )

        elif cidade == 'GARANHUNS':
            requests.post(
                'https://thingsboard.cloud/api/v1/' + token_garanhus + '/telemetry',
                data=decoded_data
            )
        elif cidade == 'SURUBIM':
            requests.post(
                'https://thingsboard.cloud/api/v1/' + token_surubim + '/telemetry',
                data=decoded_data
            )
        elif cidade == 'CARUARU':
            requests.post(
                'https://thingsboard.cloud/api/v1/' + token_caruaru + '/telemetry',
                data=decoded_data
            )
        elif cidade == 'IBIMIRIM':
            requests.post(
                'https://thingsboard.cloud/api/v1/' + token_ibimirim + '/telemetry',
                data=decoded_data
            )
        elif cidade == 'SERRA TALHADA':
            requests.post(
                'https://thingsboard.cloud/api/v1/' + token_serra_talhada + '/telemetry',
                data=decoded_data
            )
        elif cidade == 'FLORESTA':
            requests.post(
                'https://thingsboard.cloud/api/v1/' + token_floresta + '/telemetry',
                data=decoded_data
            )
        elif cidade == 'PALMARES':
            requests.post(
                'https://thingsboard.cloud/api/v1/' + token_palmares + '/telemetry',
                data=decoded_data
            )
        elif cidade == 'OURICURI':
            requests.post(
                'https://thingsboard.cloud/api/v1/' + token_ouricuri + '/telemetry',
                data=decoded_data
            )
        elif cidade == 'SALGUEIRO':
            requests.post(
                'https://thingsboard.cloud/api/v1/' + token_salgueiro + '/telemetry',
                data=decoded_data
            )
        else:
            requests.post(
                'https://thingsboard.cloud/api/v1/' + token_cabrobo + '/telemetry',
                data=decoded_data
            )

def main(event, context):
    thingsboard(event['Records'])
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
