import json
import logging
import requests
from custom_encoder import CustomEncoder
logger = logging.getLogger()
logger.setLevel(logging.INFO)

getMethod = "GET"

stocksPath = "/stocks"
stocksPricesPath = "/stocks-prices"

stocksApiPath = "https://api.polygon.io/v3/reference/tickers"
apiKey = "zK4PZnciphgwbnGWE06gT3TugVkR0CKa" 

def lambda_handler (event, context):
    logger.info(event)
    httpMethod = event['httpMethod']
    path = event['path']
    if httpMethod == getMethod and path == stocksPath:
        requestParams = event['queryStringParameters']
        response = getStocks(requestParams)
    elif httpMethod == getMethod and path == stocksPricesPath:
        requestParams = event['queryStringParameters']
        response = getStockpPriceIntoTimeframe(requestParams)
    else:
        response = buildResponse(404, 'Not Found')
    return response

def getStockpPriceIntoTimeframe(requestParams):
    try:
        stocksPricePath = """https://api.polygon.io/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{start_date}/{end_date}""".format(stocksTicker = requestParams.get("stocksTicker"), 
        multiplier = requestParams.get("multiplier"), timespan = requestParams.get("timespan"), start_date = requestParams.get("from"), end_date = requestParams.get("to"))
        params = {
            "apiKey": apiKey
        }
        params["sort"] = requestParams.get("sort") if requestParams.get("sort") is not None else "asc"
        params["limit"] = requestParams.get("limit") if requestParams.get("limit") is not None else 120
        response = requests.request("GET", stocksPricePath, params=params)
        body = {
            'stocksPrices': response.json()
        }
        return buildResponse(200, body)
    except:
        logger.exception('Bad request')


def getStocks(requestParams):
    try:
        params = {
            "apiKey": apiKey,
        }
        params["active"] = requestParams.get("active") if requestParams.get("active") is not None else "true"
        params["sort"] = requestParams.get("sort") if requestParams.get("sort") is not None else "ticker"
        params["order"] = requestParams.get("order") if requestParams.get("order") is not None else "asc"
        params["limit"] = requestParams.get("limit")  if requestParams.get("limit")  is not None else 10

        response = requests.request("GET", stocksApiPath, params=params)
        body = {
            'stocks': response.json() 
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