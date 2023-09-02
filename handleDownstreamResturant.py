import os
import boto3
import logging
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime
from bson.json_util import dumps
from model.secrets import get_secret

load_dotenv()

mongo_secret = get_secret(secret_name=os.getenv("MONGO_ATLAS_SECRET"))
sns = boto3.client("sns", region_name=os.getenv("AWS_REGION"))
dynamodb = boto3.client("dynamodb", region_name=os.getenv("AWS_REGION"))


def lambda_handler(events, context):
    client = MongoClient(mongo_secret.get("mongo_uri"))
    db = client["fairepay"]

    current_time = datetime.utcnow()

    data = dynamodb.get_item(
        TableName=os.getenv("DYNAMODB_TABLE"),
        Key={"id": {"S": "RESTAURANT_LATEST_TIME"}},
    )

    if "Item" in data:
        latest_time = datetime.fromtimestamp(float(data["Item"]["data"]["S"]))
    else:
        latest_time = datetime.fromtimestamp(0)

    collection = db.get_collection("restaurant")
    response = collection.find(
        {
            "modified_at": {
                "$lt": current_time,
                "$gte": latest_time,
            }
        }
    )

    for resp in response:
        msg_data = {
            "id": resp["_id"],
            "restaurant_id": resp["restaurant_id"],
            "modified_at": resp["modified_at"],
        }

        response = sns.publish(
            TopicArn=os.getenv("SNS_TOPIC_ARN"),
            Message=dumps(msg_data),
            MessageDeduplicationId=str(resp["_id"]),
            MessageGroupId="restaurant",
            MessageAttributes={
                "group": {
                    "DataType": "String",
                    "StringValue": "restaurant",
                }
            },
        )

    dynamodb.update_item(
        TableName=os.getenv("DYNAMODB_TABLE"),
        Key={"id": {"S": "RESTAURANT_LATEST_TIME"}},
        AttributeUpdates={"data": {"Value": {"S": f"{current_time.timestamp()}"}}},
    )


if __name__ == "__main__":
    lambda_handler(events={}, context="")
