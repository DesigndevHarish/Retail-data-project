import json
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer


# ============================================================
# PROJECT PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent

PRODUCTS_FILE = BASE_DIR / "data" / "master" / "products.csv"
STORES_FILE = BASE_DIR / "data" / "master" / "stores.csv"
CUSTOMERS_FILE = BASE_DIR / "data" / "master" / "customers.csv"


# ============================================================
# REDPANDA CONFIGURATION
# ============================================================

KAFKA_BROKER = "localhost:19092"
TOPIC = "retail-events"


producer = Producer(
    {
        "bootstrap.servers": KAFKA_BROKER
    }
)


# ============================================================
# LOAD MASTER DATA
# ============================================================

products = pd.read_csv(PRODUCTS_FILE)
stores = pd.read_csv(STORES_FILE)
customers = pd.read_csv(CUSTOMERS_FILE)


# ============================================================
# DELIVERY CALLBACK
# ============================================================

def delivery_report(err, msg):

    if err is not None:

        print(
            f"Delivery failed: {err}"
        )

    else:

        print(
            f"Delivered → "
            f"topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


# ============================================================
# EVENT GENERATOR
# ============================================================

def generate_event():

    event_type = random.choices(
        [
            "SALE",
            "RETURN",
            "INVENTORY_RECEIVED",
            "INVENTORY_ADJUSTMENT"
        ],
        weights=[
            65,
            10,
            20,
            5
        ],
        k=1
    )[0]


    product = products.sample(1).iloc[0]
    store = stores.sample(1).iloc[0]
    customer = customers.sample(1).iloc[0]


    quantity = random.randint(1, 5)


    event = {
        "event_id": str(uuid.uuid4()),

        "event_type": event_type,

        "event_timestamp": datetime.now(
            timezone.utc
        ).isoformat(),

        "product": {
            "product_id": str(product["product_id"]),
            "product_name": str(product["product_name"]),
            "category": str(product["category"]),
            "brand": str(product["brand"])
        },

        "store": {
            "store_id": str(store["store_id"]),
            "store_name": str(store["store_name"]),
            "city": str(store["city"]),
            "state": str(store["state"])
        },

        "customer": {
            "customer_id": str(customer["customer_id"]),
            "customer_city": str(customer["city"]),
            "customer_segment": str(
                customer["customer_segment"]
            )
        },

        "quantity": quantity,

        "unit_price_inr": round(
            float(product["regular_price_inr"]),
            2
        )
    }


    return event


# ============================================================
# MAIN STREAMING LOOP
# ============================================================

def main():

    print("=" * 60)
    print("REAL-TIME RETAIL EVENT STREAM")
    print("=" * 60)

    print(
        f"Broker : {KAFKA_BROKER}"
    )

    print(
        f"Topic  : {TOPIC}"
    )

    print()

    print(
        "Streaming retail events..."
    )

    print(
        "Press Ctrl+C to stop."
    )

    print()


    try:

        while True:

            event = generate_event()

            message = json.dumps(
                event
            )

            producer.produce(
                topic=TOPIC,
		key=str(event["product"]["product_id"]),
                value=message,
                callback=delivery_report
            )

            producer.poll(0)

            print(
                f"{event['event_type']} | "
                f"{event['product']['product_id']} | "
                f"{event['store']['store_id']} | "
                f"Qty={event['quantity']}"
            )

            time.sleep(
                random.uniform(1, 5)
            )


    except KeyboardInterrupt:

        print(
            "\nStopping producer..."
        )


    finally:

        producer.flush()

        print(
            "Producer stopped."
        )


if __name__ == "__main__":

    main()