from urllib import response
import boto3
import json
from custom_encoder import CustomEncoder
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


dynamoTableName = "EmpowermentTest"
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(dynamoTableName)

getMethod = "GET"
postMethod = "POST"
patchMethod = "PATCH"
deleteMethod = "DELETE"

healthPath = "/health"
traderPath = "/trader"
tradersPath = "/traders"

def lambda_handler (event, context):
    logger.info(event)
    httpMethod = event['httpMethod']
    path = event['path']
    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200)
    elif httpMethod == getMethod and path == traderPath:
        response = getTrader(event['queryStringParameters']['traderId'])
    elif httpMethod == getMethod and path == tradersPath:
        response = getTraders()
    elif httpMethod == postMethod and path == traderPath:
        response = saveTrader(json.loads(event['body']))
    elif httpMethod == patchMethod and path == traderPath:
        requestBody = json.loads(event['body'])
        response = modifyTrader(requestBody['traderId'], requestBody['updateKey'], requestBody['updateValue'])
    elif httpMethod == deleteMethod and path == traderPath:
        requestBody = json.loads(event['body'])
        response = deleteTrader(requestBody['traderId'])
    else:
        response = buildResponse(404, 'Not Found')
    return response
def getTrader(traderId):
    try:
        response = table.get_item(
            Key ={
                'traderId': traderId
            }
        )
        if 'Item' in response:
            return buildResponse(200, response['Item'])
        else:
            return buildResponse(404, {'Message': 'traderId %s not found' % traderId})
    except:
        logger.exception('Trader not found. Input a correct Trader id')

def getTraders():
    try: 
        response = table.scan()
        result = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            result.extend(response['Items'])
        body = {
            'traders' : result 
        }
        return buildResponse(200, body)
    except:
        logger.exception('Traders not found. Input a correct Traders')
        
            
def saveTrader(requestBody):
    try:
        table.put_item(Item= requestBody)
        body = {
            'Operation': 'SAVE',
            'Message': 'SUCCESS',
            'Item': requestBody
        }
        return buildResponse(200, body)
    except:
        logger.exception('Trader not save . Input a correct Trader')

def modifyTrader(traderId, updateKey, updateValue):
    try:
        response = table.update_item(
            Key = {
                'traderId': traderId
            },
            UpdateExpression='set %s = :value' % updateKey,
            ExpressionAttributeValues = {
                ':value': updateValue
            },
            ReturnValues = 'UPDATED_NEW'
        )
        body = {
            'Operation': 'UPDATE',
            'Message': 'SUCCESS',
            'UpdatedAttrebutes': response
        }
        return buildResponse(200, body)

    except:
        logger.exception('Trader not update. Input a correct Trader information')

def deleteTrader(traderId):
    try:
        response = table.delete_item(
            Key = {
                'traderId': traderId
            }, 
            ReturnValues='ALL_OLD'
        )
        body = {
            'Operation': 'DELETE',
            'Message': 'SUCCESS',
            'DeletedItem': response
        }
        return buildResponse(200, body)
    except:
        logger.exception('Trader not delete. Input a correct Trader information')


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


