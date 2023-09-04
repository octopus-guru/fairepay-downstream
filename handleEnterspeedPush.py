import os
from json import loads, dumps
from dotenv import load_dotenv
import logging
from pymongo import MongoClient
import requests

from model.secrets import get_secret

load_dotenv()

mongo_secret = get_secret(secret_name=os.getenv("MONGO_ATLAS_SECRET"))


def lambda_handler(events, context):
    print("events", events)

    client = MongoClient(mongo_secret.get("mongo_uri"))
    db = client["fairepay"]

    for event in events.get("Records"):
        body = loads(event.get("body"))
        msg = loads(body.get("Message"))

        if (
            body.get("MessageAttributes", {}).get("group", {}).get("Value")
            == "restaurant"
        ):
            mongo_pipeline = [
                {"$match": {"restaurant_id": msg.get("restaurant_id")}},
                {
                    "$lookup": {
                        "from": "category",
                        "localField": "restaurant_id",
                        "foreignField": "restaurant_id",
                        "as": "categorys",
                    }
                },
                {
                    "$project": {
                        "restaurant_id": 1,
                        "name": 1,
                        "uri": 1,
                        "status": 1,
                        "categorys._id": 1,
                        "categorys.name": 1,
                    }
                },
                {
                    "$lookup": {
                        "from": "product",
                        "localField": "categorys._id",
                        "foreignField": "category_id",
                        "as": "products",
                    }
                },
                {
                    "$project": {
                        "restaurant_id": 1,
                        "name": 1,
                        "uri": 1,
                        "status": 1,
                        "categorys._id": 1,
                        "categorys.name": 1,
                        "products._id": 1,
                        "products.category_id": 1,
                    }
                },
            ]

            collection = db.get_collection("restaurant")
            response = collection.aggregate(pipeline=mongo_pipeline)
            for res in response:
                properties = {
                    "name": res["name"],
                    "categorys": [],
                }

                prod_cats = {}
                for prod in res["products"]:
                    if not str(prod["category_id"]) in prod_cats:
                        prod_cats[str(prod["category_id"])] = []

                    prod_cats[str(prod["category_id"])].append(str(prod["_id"]))

                for cat in res["categorys"]:
                    if not str(cat["_id"]) in prod_cats:
                        prod_cats[str(prod["category_id"])] = []

                    properties["categorys"].append(
                        {
                            "_id": str(cat["_id"]),
                            "name": cat["name"],
                            "products": prod_cats[str(cat["_id"])],
                        }
                    )

                resp = requests.post(
                    url=f"{os.getenv('ENTERSPEED_INGEST_URL')}{res['restaurant_id']}",
                    headers={
                        "X-Api-Key": os.getenv("ENTERSPEED_API_KEY"),
                    },
                    data=dumps(
                        {
                            "type": "restaurentMenu",
                            "url": f"{os.getenv('FRONTEND_DOMAIN')}/menu/{res['uri']}",
                            "properties": properties,
                        }
                    ),
                )

                if resp.status_code > 200 or resp.status_code < 299:
                    logging.warning(f"http status code: {resp.status_code}")

        else:
            logging.warning("Order already has been handlet.")


if __name__ == "__main__":
    with open("tests/dummyEvents/resturantEvent.json") as f:
        contents = loads(f.read())

        lambda_handler(events=contents, context="")
