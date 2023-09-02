import os
from json import loads
from dotenv import load_dotenv
import logging
from pymongo import MongoClient

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

        print("Message", msg)

        if msg.get("event") in ["payment.checkout.completed"]:
            collection = db.get_collection("restaurant_order")

            row = collection.find_one(
                {
                    "payment_id": msg.get("data", {}).get("paymentId"),
                }
            )

            if not row:
                logging.error("Payment_id not exists.")

            elif (
                row["order_status"] == "AWAITING-PAYMENT"
                and row["payment_status"] == "PENDING"
            ):
                logging.info("Order updated and ready for resturant to be accepted")

                collection.update_one(
                    {
                        "payment_id": msg.get("data", {}).get("paymentId"),
                    },
                    {"$set": {"order_status": "PENDING", "payment_status": "READY"}},
                )

            else:
                logging.warning("Order already has been handlet.")


# if __name__ == "__main__":
#     with open("tests/dummyEvents/payment.checkout.completed.json") as f:
#         contents = loads(f.read())

#         lambda_handler(events=contents, context=_)
