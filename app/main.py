from fastapi import FastAPI, HTTPException
from fastapi.logger import logger
from fastapi.responses import RedirectResponse

import json
import logging
import requests
import boto3
import os
import uuid
import datetime
import pymysql

# DB_ENDPOINT = os.environ.get('db_endpoint')
# DB_ADMIN_USER = os.environ.get('db_admin_user')
# DB_ADMIN_PASSWORD = os.environ.get('db_admin_password')
# DB_NAME = os.environ.get('db_name')

app = FastAPI(title='Click Service', version='0.1')
gunicorn_logger = logging.getLogger('gunicorn.error')
logger.handlers = gunicorn_logger.handlers
logger.setLevel(gunicorn_logger.level)

tracking_click_endpoint = 'http://internal-private-1191134035.us-east-2.elb.amazonaws.com/tracking/click'


if __name__ != "main":
    logger.setLevel(gunicorn_logger.level)
else:
    logger.setLevel(logging.DEBUG)


# def get_db_conn():
#     try:
#         conn = pymysql.connect(host=DB_ENDPOINT, user=DB_ADMIN_USER, passwd=DB_ADMIN_PASSWORD, db=DB_NAME, connect_timeout=5)
#         return conn
#     except pymysql.MySQLError as e:
#         logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
#         logger.error(e)
#         raise

    
@app.get("/")
def read_root():
    return {"Service": "Click"}


@app.get("/table")
def table():
    client = boto3.client('dynamodb')

    response = client.describe_table(
        TableName='sessions'
    )

    return {"Service": response}

@app.get("/click", response_class=RedirectResponse, status_code=302)
async def click(query_id: str, impression_id: str):
    try:
        now_timestamp_iso = datetime.datetime.now()
        
        # click
        dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
        table = dynamodb.Table('sessions')
        response = table.get_item(
            Key={
                'query_id': query_id,
                'impression_id': impression_id
            }
        )
        advertiser_url = response['Item']['advertiser_url']
        publisher_id = response['Item']['publisher_id']
        advertiser_id = response['Item']['advertiser_id']
        campaign_id = response['Item']['campaign_id']
        category = response['Item']['category']
        ad_id = response['Item']['ad_id']
        zip_code = response['Item']['zip_code']
        advertiser_price = response['Item']["advertiser_price"]
        publisher_price = response['Item']["publisher_price"]
        position = response['Item']["position"]

        # track click
        click_id = str(uuid.uuid4())
        
        # # using db connection
        # with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        #     sql_query = """SELECT campaign_id FROM ads WHERE advertiser_url = %s"""
        #     cursor.execute(sql_query, (advertiser_url))
        #     campaign_id = cursor.fetchone()
            
        #     sql_query = """SELECT advertiser_id FROM advertiser_campaigns WHERE id = %s"""
        #     cursor.execute(sql_query, (campaign_id))
        #     advertiser_id = cursor.fetchone()
            
        #     sql_query = """SELECT publisher_id FROM publisher_exclusions WHERE advertiser_id = %s"""
        #     cursor.execute(sql_query, (advertiser_id))
        #     publisher_id = cursor.fetchone()
            
        #     sql_query4 = """SELECT category FROM advertiser_campaigns WHERE id = %s"""
        #     cursor.execute(sql_query4, (campaign_id))
        #     category = cursor.fetchone()
            
        #     sql_query = """SELECT id FROM ads WHERE url = %s"""
        #     cursor.execute(sql_query, (advertiser_url))
        #     ad_id = cursor.fetchone()
            
        #     sql_query6 = """SELECT zip_code FROM campaign_targeting WHERE campaign_id = %s"""
        #     cursor.execute(sql_query6, (campaign_id))
        #     zip_code = cursor.fetchone()
        
        tracking_click_params = {
            "query_id": str(query_id),
            "impression_id": str(impression_id),
            "click_id": str(click_id),
            "timestamp": str(now_timestamp_iso),
            "publisher_id": int(publisher_id),
            "advertiser_id": int(advertiser_id),
            "advertiser_campaign_id": int(campaign_id),
            "category": int(category),
            "ad_id": int(ad_id),
            "zip_code": str(zip_code),
            "advertiser_price": float(advertiser_price),
            "publisher_price": float(publisher_price),
            "position": int(position)
        }
        
        tracking_click_response = requests.post(tracking_click_endpoint, json=tracking_click_params)
        tracking_click_response.raise_for_status()
        logger.error(tracking_click_response)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    return advertiser_url
