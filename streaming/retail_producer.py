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
# BUSINESS CONFIGURATION
# ============================================================

PAYMENT_METHODS = [
    "UPI",
    "CREDIT_CARD",
    "DEBIT_CARD",
    "CASH",
    "NET_BANKING",
]


# ============================================================
# LOAD MASTER DATA
# ============================================================

products = pd.read_csv(PRODUCTS_FILE)
stores = pd.read_csv(STORES_FILE)
customers = pd.read_csv(CUSTOMERS_FILE)


# ============================================================
# LIVE INVENTORY STATE
# ============================================================

inventory_state = {}


def generate_initial_inventory(products_df, stores_df):

    inventory = {}

    for _, product in products_df.iterrows():

        product_id = str(
            product["product_id"]
        )

        for _, store in stores_df.iterrows():

            store_id = str(
                store["store_id"]
            )

            inventory[(product_id, store_id)] = random.randint(
                20,
                100
            )

    return inventory


inventory_state = generate_initial_inventory(
    products,
    stores
)


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

    # --------------------------------------------------------
    # Select event type
    # --------------------------------------------------------

    event_type = random.choices(
        [
            "SALE",
            "RETURN",
            "INVENTORY_RECEIVED",
            "INVENTORY_ADJUSTMENT"
        ],
        weights=[
            65,
            8,
            20,
            7
        ],
        k=1
    )[0]


    # --------------------------------------------------------
    # Select master data
    # --------------------------------------------------------

    product = products.sample(1).iloc[0]
    store = stores.sample(1).iloc[0]
    customer = customers.sample(1).iloc[0]


    product_id = str(
        product["product_id"]
    )

    store_id = str(
        store["store_id"]
    )


    # --------------------------------------------------------
    # Inventory key
    # --------------------------------------------------------

    inventory_key = (
        product_id,
        store_id
    )


    # --------------------------------------------------------
    # Previous inventory
    # --------------------------------------------------------

    previous_quantity = inventory_state[
        inventory_key
    ]


    # --------------------------------------------------------
    # Determine inventory movement
    # --------------------------------------------------------

    if event_type == "SALE":

        quantity_change = -random.randint(
            1,
            min(
                5,
                max(
                    1,
                    previous_quantity
                )
            )
        )


    elif event_type == "RETURN":

        quantity_change = random.randint(
            1,
            3
        )


    elif event_type == "INVENTORY_RECEIVED":

        quantity_change = random.randint(
            10,
            100
        )


    else:

        # INVENTORY_ADJUSTMENT

        quantity_change = random.randint(
            -5,
            5
        )

        # Do not allow zero adjustment
        if quantity_change == 0:

            quantity_change = 1

        # Do not allow inventory to become negative
        quantity_change = max(
            -previous_quantity,
            quantity_change
        )


    # --------------------------------------------------------
    # Transaction quantity
    #
    # Same logic as historical producer:
    # quantity is always the absolute movement.
    # --------------------------------------------------------

    transaction_quantity = abs(
        quantity_change
    )


    # --------------------------------------------------------
    # Current inventory
    # --------------------------------------------------------

    current_quantity = max(
        0,
        previous_quantity + quantity_change
    )


    # Update live inventory state

    inventory_state[inventory_key] = (
        current_quantity
    )


    # --------------------------------------------------------
    # Inventory availability
    # --------------------------------------------------------

    if current_quantity == 0:

        availability = "OUT_OF_STOCK"

    elif current_quantity <= 10:

        availability = "LOW_STOCK"

    else:

        availability = "IN_STOCK"


    # --------------------------------------------------------
    # Product pricing
    #
    # Match historical producer:
    # regular price + small realistic variation
    # --------------------------------------------------------

    unit_price = float(
        product["regular_price_inr"]
    )


    selling_price = round(
        unit_price
        * random.uniform(
            0.90,
            1.05
        ),
        2
    )


    # --------------------------------------------------------
    # Payment method
    #
    # Match historical producer:
    # only SALE receives a payment method.
    # --------------------------------------------------------

    payment_method = (
        random.choice(PAYMENT_METHODS)
        if event_type == "SALE"
        else None
    )


    # --------------------------------------------------------
    # Build canonical event
    # --------------------------------------------------------

    event = {

        # ----------------------------------------------------
        # Event
        # ----------------------------------------------------

        "event_id": str(
            uuid.uuid4()
        ),

        "event_type": event_type,

        "event_timestamp": datetime.now(
            timezone.utc
        ).isoformat(),


        # ----------------------------------------------------
        # Product
        # ----------------------------------------------------

        "product": {

            "product_id": product_id,

            "product_name": str(
                product["product_name"]
            ),

            "brand": str(
                product["brand"]
            ),

            "category": str(
                product["category"]
            )
        },


        # ----------------------------------------------------
        # Store
        # ----------------------------------------------------

        "store": {

            "store_id": store_id,

            "store_name": str(
                store["store_name"]
            ),

            "city": str(
                store["city"]
            ),

            "state": str(
                store["state"]
            ),

            "country": str(
                store["country"]
            )
        },


        # ----------------------------------------------------
        # Customer
        # ----------------------------------------------------

        "customer": {

            "customer_id": str(
                customer["customer_id"]
            ),

            "customer_city": str(
                customer["city"]
            ),

            "customer_segment": str(
                customer["customer_segment"]
            )
        },


        # ----------------------------------------------------
        # Inventory
        # ----------------------------------------------------

        "inventory": {

            "previous_quantity": previous_quantity,

            "quantity_change": quantity_change,

            "current_quantity": current_quantity,

            "availability": availability
        },


        # ----------------------------------------------------
        # Transaction
        # ----------------------------------------------------

        "transaction": {

            "quantity": transaction_quantity,

            "unit_price_inr": selling_price,

            "currency": "INR",

            "payment_method": payment_method
        }
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


            # ------------------------------------------------
            # Convert event to JSON
            # ------------------------------------------------

            message = json.dumps(
                event
            )


            # ------------------------------------------------
            # Kafka message
            #
            # Product ID is used as the Kafka key so events
            # for the same product are consistently partitioned.
            # ------------------------------------------------

            producer.produce(

                topic=TOPIC,

                key=str(
                    event["product"]["product_id"]
                ),

                value=message,

                callback=delivery_report
            )


            producer.poll(0)


            # ------------------------------------------------
            # Console output
            # ------------------------------------------------

            print(
                f"{event['event_type']} | "
                f"{event['product']['product_id']} | "
                f"{event['store']['store_id']} | "
                f"Qty={event['transaction']['quantity']} | "
                f"Change={event['inventory']['quantity_change']} | "
                f"Price={event['transaction']['unit_price_inr']}"
            )


            # ------------------------------------------------
            # Realistic event interval
            # ------------------------------------------------

            time.sleep(
                random.uniform(
                    1,
                    5
                )
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


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()
