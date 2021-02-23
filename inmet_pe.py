from datetime import datetime, timezone, timedelta
import requests
import json
import boto3


def buscar_dados_pe(data, hora):
    request = requests.get(
        'https://apitempo.inmet.gov.br/estacao/dados/{}/{}'
        .format(data, hora)
    )
    estacoes = json.loads(request.content)
    parametros = ['DC_NOME', 'TEM_INS', 'UMD_INS']
    pe = []
    for e in estacoes:
        if e['UF'] == 'PE':
            _e = {p: e[p] for p in parametros}
            pe.append(_e)
    return pe


def stream_dados(dados):
    client = boto3.client('kinesis')
    client.put_record(
        StreamName='input_stream',
        Data=json.dumps(dados),
        PartitionKey='PartitionKey'
    )


def main(event, context):
    timestamp = datetime.now()
    fuso_horario = timezone(timedelta(hours=-3))
    time_recife = timestamp.astimezone(fuso_horario)
    data = time_recife.strftime('%Y-%m-%d')
    hora = time_recife.strftime('%H') + '00'
    dados_pe = buscar_dados_pe(data, hora)
    stream_dados(dados_pe)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
