from distutils.command.config import dump_file
import json
import boto3
import logging
import requests
import pandas as pd
from custom_encoder import CustomEncoder
logger = logging.getLogger()
logger.setLevel(logging.INFO)


getMethod = "GET"

stocksApiPath = "https://api.polygon.io/v3/reference/tickers"

stocksCSVPath = "/stocks-csv"
apiKey = "zK4PZnciphgwbnGWE06gT3TugVkR0CKa" 


def lambda_handler (event, context):
    logger.info(event)
    httpMethod = event['httpMethod']
    path = event['path']
    if httpMethod == getMethod and path == stocksCSVPath:
        requestParams = event['queryStringParameters']
        response = generateCSV(requestParams)
    else:
        response = buildResponse(404, 'Not Found')
    return response

def generateCSV(requestParams):
    try:
        params = {
            "apiKey": apiKey,
        }
        params["active"] = requestParams.get("active") if requestParams.get("active") is not None else "true"
        params["sort"] = requestParams.get("sort") if requestParams.get("sort") is not None else "ticker"
        params["order"] = requestParams.get("order") if requestParams.get("order") is not None else "asc"
        params["limit"] = requestParams.get("limit")  if requestParams.get("limit")  is not None else 10
        response = requests.request("GET", stocksApiPath, params=params)
        df = pd.json_normalize(response.json()['results'])
        df.to_csv("s3://stocks-empowerment-123/stocks-csv/stocks-information.csv")
        body = {
            'Operation': 'SAVE',
            'Message': 'SUCCESS'
            }
        return buildResponse(200, body)
    except:
        logger.exception('Bad request')

def buildResponse(statusCode, body = None):
    response = {
        'statusCode': statusCode, 
        'headers': { 
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            }
    }
    
    if body is not None:
        response['body'] = json.dumps(body, cls=CustomEncoder)
    return response
if __name__ == '__main__':
    params = {
        "active": "true",
        "sort": "ticker"
    }
    generateCSV(params)