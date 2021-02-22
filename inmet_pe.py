import requests
import json
import boto3
import time

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
    response = client.put_record(
        StreamName = 'input_stream',
        Data = json.dumps(dados),
        PartitionKey = 'PartitionKey'
    )
    return response


def main(event, context):
    data = event['d']
    hora = event['h']
    dados_pe = buscar_dados_pe(data, hora)
    stream_dados(dados_pe)
    return dados_pe
