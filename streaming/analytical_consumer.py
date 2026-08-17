import json
from collections import Counter, defaultdict

from confluent_kafka import Consumer, KafkaException


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BROKER = "localhost:19092"
KAFKA_TOPIC = "retail-events"
CONSUMER_GROUP = "analytics_consumer"


# ============================================================
# KAFKA CONSUMER
# ============================================================

consumer = Consumer(
    {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": CONSUMER_GROUP,

        # New consumer group:
        # start from the earliest available event.
        "auto.offset.reset": "earliest",

        # We commit manually.
        "enable.auto.commit": False,
    }
)


# ============================================================
# ANALYTICS STATE
# ============================================================

total_events = 0

event_type_count = Counter()

store_events = Counter()

product_events = Counter()

total_units_sold = 0

total_revenue = 0.0


# ============================================================
# DISPLAY ANALYTICS
# ============================================================

def display_metrics():

    print()
    print("=" * 65)
    print("REAL-TIME RETAIL ANALYTICS")
    print("=" * 65)

    print(
        f"Events processed : {total_events}"
    )

    print()

    print("Event Types")

    for event_type, count in event_type_count.items():

        print(
            f"  {event_type:<25} {count}"
        )

    print()

    print(
        f"Units sold       : {total_units_sold}"
    )

    print(
        f"Revenue          : ₹{total_revenue:,.2f}"
    )

    print()

    print("Top Stores")

    for store_id, count in store_events.most_common(5):

        print(
            f"  {store_id:<15} {count} events"
        )

    print()

    print("Top Products")

    for product_id, count in product_events.most_common(5):

        print(
            f"  {product_id:<15} {count} events"
        )

    print("=" * 65)
    print()


# ============================================================
# PROCESS EVENT
# ============================================================

def process_event(event):

    global total_events
    global total_units_sold
    global total_revenue

    total_events += 1

    event_type = event.get(
        "event_type",
        "UNKNOWN"
    )

    event_type_count[event_type] += 1


    # --------------------------------------------------------
    # Store
    # --------------------------------------------------------

    store_id = event.get("store_id")

    if not store_id:

        store_data = event.get(
            "store",
            {}
        )

        store_id = store_data.get(
            "store_id"
        )

    if store_id:

        store_events[store_id] += 1


    # --------------------------------------------------------
    # Product
    # --------------------------------------------------------

    product_id = event.get("product_id")

    if not product_id:

        product_data = event.get(
            "product",
            {}
        )

        product_id = product_data.get(
            "product_id"
        )

    if product_id:

        product_events[product_id] += 1


    # --------------------------------------------------------
    # Quantity
    # --------------------------------------------------------

    quantity = event.get(
        "quantity",
        0
    )

    try:

        quantity = int(quantity)

    except (ValueError, TypeError):

        quantity = 0


    # --------------------------------------------------------
    # Sales
    # --------------------------------------------------------

    if event_type == "SALE":

        total_units_sold += quantity

        unit_price = event.get(
            "unit_price_inr",
            0
        )

        try:

            unit_price = float(
                unit_price
            )

        except (ValueError, TypeError):

            unit_price = 0

        total_revenue += (
            quantity * unit_price
        )


    # --------------------------------------------------------
    # Returns
    # --------------------------------------------------------

    elif event_type == "RETURN":

        total_units_sold -= quantity

        unit_price = event.get(
            "unit_price_inr",
            0
        )

        try:

            unit_price = float(
                unit_price
            )

        except (ValueError, TypeError):

            unit_price = 0

        total_revenue -= (
            quantity * unit_price
        )


# ============================================================
# MAIN
# ============================================================

def main():

    consumer.subscribe(
        [KAFKA_TOPIC]
    )

    print("=" * 65)
    print("LOCAL REAL-TIME ANALYTICS CONSUMER")
    print("=" * 65)

    print(
        f"Broker        : {KAFKA_BROKER}"
    )

    print(
        f"Topic         : {KAFKA_TOPIC}"
    )

    print(
        f"Consumer Group: {CONSUMER_GROUP}"
    )

    print()

    print(
        "Waiting for retail events..."
    )

    print(
        "Press Ctrl+C to stop."
    )

    print()


    try:

        while True:

            message = consumer.poll(
                1.0
            )


            # ------------------------------------------------
            # No message
            # ------------------------------------------------

            if message is None:

                continue


            # ------------------------------------------------
            # Kafka error
            # ------------------------------------------------

            if message.error():

                raise KafkaException(
                    message.error()
                )


            # ------------------------------------------------
            # Parse JSON
            # ------------------------------------------------

            try:

                event = json.loads(
                    message.value().decode(
                        "utf-8"
                    )
                )

            except json.JSONDecodeError as error:

                print(
                    f"Invalid JSON: {error}"
                )

                # Skip invalid event.
                consumer.commit(
                    message=message,
                    asynchronous=False
                )

                continue


            # ------------------------------------------------
            # Process event
            # ------------------------------------------------

            process_event(
                event
            )


            print(
                f"Processed | "
                f"partition={message.partition()} | "
                f"offset={message.offset()} | "
                f"type={event.get('event_type')} | "
                f"events={total_events}"
            )


            # ------------------------------------------------
            # Commit after processing
            # ------------------------------------------------

            consumer.commit(
                message=message,
                asynchronous=False
            )


            # ------------------------------------------------
            # Display metrics every 10 events
            # ------------------------------------------------

            if total_events % 10 == 0:

                display_metrics()


    except KeyboardInterrupt:

        print()
        print(
            "Stopping analytics consumer..."
        )


    finally:

        consumer.close()

        print(
            "Analytics consumer stopped."
        )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    main()

