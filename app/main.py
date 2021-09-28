from typing import Optional,List

from fastapi import FastAPI, HTTPException
from fastapi.logger import logger
from fastapi.param_functions import Query
from fastapi.responses import RedirectResponse

import  pymysql
import os
import logging 

import boto3

# DB_ENDPOINT = os.environ.get('db_endpoint')
# DB_ADMIN_USER = os.environ.get('db_admin_user')
# DB_ADMIN_PASSWORD = os.environ.get('db_admin_password')
# DB_NAME = os.environ.get('db_name')

# AWS_ACCESS_KEY_ID =''
# AWS_SECRET_ACCESS_KEY=''

app = FastAPI(title='Matching Service',version='0.1')
gunicorn_logger = logging.getLogger('gunicorn.error')
logger.handlers = gunicorn_logger.handlers
logger.setLevel(gunicorn_logger.level)

if __name__ != "main":
    logger.setLevel(gunicorn_logger.level)
else:
    logger.setLevel(logging.DEBUG)

@app.get("/")
def read_root():
    return {"Service": "click"}

@app.get("/table")
def read_root():
    client = boto3.client('dynamodb')

    response = client.describe_table(
        TableName='sessions'
    )

    return {"Service": response}


@app.get("/click",response_class=RedirectResponse, status_code=302)
async def click(query_id:str,impression_id:str):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    table = dynamodb.Table('sessions')
    response = table.get_item(
        Key={
            'query_id': query_id,
            'impression_id': impression_id
        }
    )
    item = response['Item']['advertiser_url']
    return item