import boto3
import json
import logging
import time
from boto3.dynamodb.conditions import Key, Attr

logger = logging.getLogger("handler_logger")
logger.setLevel(logging.DEBUG)
dynamodb = boto3.resource("dynamodb")

def _get_response(status_code, body):
    if not isinstance(body, str):
        body = json.dumps(body)
    return {"statusCode": status_code, "body": body}

def _send_to_connection(connection_id, data, event):
    gatewayapi = boto3.client("apigatewaymanagementapi",
            endpoint_url = "https://" + event["requestContext"]["domainName"] +
                    "/" + event["requestContext"]["stage"])
    return gatewayapi.post_to_connection(ConnectionId=connection_id,
            Data=json.dumps(data).encode('utf-8'))

def connection_manager(event, context):
    """
    Handles connecting and disconnecting for the Websocket.
    """
    connectionID = event["requestContext"].get("connectionId")
    jwtToken = event["headers"].get("token")
    if( not jwtToken):
        jwtToken = "None"

    logger.debug(event["requestContext"])
    logger.debug(jwtToken)
    
    if event["requestContext"]["eventType"] == "CONNECT":
        logger.info("Connect requested")

        # Add connectionID to the database
        table = dynamodb.Table("WebsocketJWTToken")
        table.put_item(Item={"ConnectionID": connectionID, "Token": jwtToken})
        return _get_response(200, "Connect successful.")

    elif event["requestContext"]["eventType"] == "DISCONNECT":
        logger.info("Disconnect requested")

        # Remove the connectionID from the database
        table = dynamodb.Table("WebsocketJWTToken")
        table.delete_item(Key={"ConnectionID": connectionID})
        return _get_response(200, "Disconnect successful.")

    else:
        logger.error("Connection manager received unrecognized eventType '{}'")
        return _get_response(500, "Unrecognized eventType.")

def get_recent_messages(event, context):
    logger.info("Retrieving most recent messages.")
    connectionID = event["requestContext"].get("connectionId")

    # Get the 10 most recent chat messages
    table = dynamodb.Table("WebsocketJWTToken")
    response = table.query(
        KeyConditionExpression=Key('ConnectionID').eq(event["requestContext"].get("connectionId"))
    )
    items = response['Items']
    logger.debug(items)

    # Extract the relevant data and order chronologically
    # messages = [{"username": x["Username"], "content": x["Content"]}
    #         for x in items]
    # messages.reverse()

    data = {"messages": items}
    _send_to_connection(connectionID, data, event)

    return _get_response(200, "Sent recent messages.")