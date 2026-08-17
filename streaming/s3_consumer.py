import json
import time
from datetime import datetime, timezone

import boto3
from confluent_kafka import Consumer, KafkaException


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BROKER = "localhost:19092"
KAFKA_TOPIC = "retail-events"
CONSUMER_GROUP = "s3_ingestion_consumer"

S3_BUCKET = "de-streaming-json-lake-2026-480749289287-ap-south-1-an"
S3_PREFIX = "historical/retail_events"

BATCH_INTERVAL_SECONDS = 30
MAX_BATCH_SIZE = 100


# ============================================================
# AWS S3 CLIENT
# ============================================================

s3 = boto3.client("s3")


# ============================================================
# KAFKA CONSUMER
# ============================================================

consumer = Consumer(
    {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": CONSUMER_GROUP,

        # First time this group runs:
        # read the earliest available event.
        "auto.offset.reset": "earliest",

        # IMPORTANT:
        # We control commits manually.
        "enable.auto.commit": False,

        # Helps the consumer detect failures.
        "session.timeout.ms": 10000,
    }
)


# ============================================================
# S3 UPLOAD
# ============================================================

def upload_batch_to_s3(events):

    if not events:
        return False

    now = datetime.now(timezone.utc)

    date_partition = now.strftime("%Y-%m-%d")
    hour_partition = now.strftime("%H")

    timestamp = now.strftime("%Y%m%d_%H%M%S_%f")

    file_name = f"events_{timestamp}.json"

    s3_key = (
        f"{S3_PREFIX}/"
        f"date={date_partition}/"
        f"hour={hour_partition}/"
        f"{file_name}"
    )

    # JSON Lines:
    # One JSON event per line.
    body = "\n".join(
        json.dumps(
            event,
            separators=(",", ":")
        )
        for event in events
    )

    print()
    print("=" * 65)
    print("S3 UPLOAD")
    print("=" * 65)

    print(f"Events : {len(events)}")
    print(f"Bucket : {S3_BUCKET}")
    print(f"Key    : {s3_key}")

    try:

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=body.encode("utf-8"),
            ContentType="application/x-ndjson"
        )

        print("S3 upload SUCCESS")
        print("=" * 65)
        print()

        return True

    except Exception as error:

        print("S3 upload FAILED")
        print(f"Error: {error}")
        print("=" * 65)
        print()

        return False


# ============================================================
# MAIN
# ============================================================

def main():

    consumer.subscribe([KAFKA_TOPIC])

    print("=" * 65)
    print("KAFKA → S3 INGESTION CONSUMER")
    print("=" * 65)

    print(f"Broker        : {KAFKA_BROKER}")
    print(f"Topic         : {KAFKA_TOPIC}")
    print(f"Consumer Group: {CONSUMER_GROUP}")
    print(f"S3 Bucket     : {S3_BUCKET}")
    print(f"S3 Prefix     : {S3_PREFIX}")

    print()
    print("Batch interval :", BATCH_INTERVAL_SECONDS, "seconds")
    print("Maximum batch :", MAX_BATCH_SIZE, "events")

    print()
    print("Consumer is running...")
    print("Press Ctrl+C to stop.")
    print()

    events = []

    last_upload_time = time.time()

    try:

        while True:

            message = consumer.poll(1.0)

            # ------------------------------------------------
            # No Kafka message
            # ------------------------------------------------

            if message is None:

                elapsed = (
                    time.time()
                    - last_upload_time
                )

                if (
                    events
                    and elapsed >= BATCH_INTERVAL_SECONDS
                ):

                    if upload_batch_to_s3(events):

                        consumer.commit(
                            asynchronous=False
                        )

                        print(
                            f"Committed {len(events)} Kafka events."
                        )

                        events.clear()

                        last_upload_time = time.time()

                continue


            # ------------------------------------------------
            # Kafka error
            # ------------------------------------------------

            if message.error():

                raise KafkaException(
                    message.error()
                )


            # ------------------------------------------------
            # Read Kafka event
            # ------------------------------------------------

            try:

                event = json.loads(
                    message.value().decode("utf-8")
                )

            except json.JSONDecodeError as error:

                print(
                    f"Invalid JSON at "
                    f"partition={message.partition()} "
                    f"offset={message.offset()}"
                )

                print(f"Error: {error}")

                # We don't add invalid data to S3.
                # Commit this bad message so it doesn't
                # repeatedly block the consumer.
                consumer.commit(
                    message=message,
                    asynchronous=False
                )

                continue


            # ------------------------------------------------
            # Add Kafka lineage information
            # ------------------------------------------------

            event["_kafka"] = {
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset()
            }


            events.append(event)


            print(
                f"Received | "
                f"partition={message.partition()} | "
                f"offset={message.offset()} | "
                f"type={event.get('event_type')} | "
                f"batch={len(events)}"
            )


            # ------------------------------------------------
            # MAX BATCH SIZE
            # ------------------------------------------------

            if len(events) >= MAX_BATCH_SIZE:

                if upload_batch_to_s3(events):

                    consumer.commit(
                        asynchronous=False
                    )

                    print(
                        f"Committed {len(events)} Kafka events."
                    )

                    events.clear()

                    last_upload_time = time.time()


            # ------------------------------------------------
            # TIME BASED BATCH
            # ------------------------------------------------

            elapsed = (
                time.time()
                - last_upload_time
            )

            if (
                events
                and elapsed >= BATCH_INTERVAL_SECONDS
            ):

                if upload_batch_to_s3(events):

                    consumer.commit(
                        asynchronous=False
                    )

                    print(
                        f"Committed {len(events)} Kafka events."
                    )

                    events.clear()

                    last_upload_time = time.time()


    # ========================================================
    # CTRL+C
    # ========================================================

    except KeyboardInterrupt:

        print()
        print("Stopping consumer...")


    # ========================================================
    # CLEAN SHUTDOWN
    # ========================================================

    finally:

        # Attempt to upload anything still in memory.
        if events:

            print()
            print(
                f"Final batch contains "
                f"{len(events)} events."
            )

            try:

                if upload_batch_to_s3(events):

                    consumer.commit(
                        asynchronous=False
                    )

                    print(
                        "Final batch uploaded "
                        "and offsets committed."
                    )

                else:

                    print(
                        "Final upload failed."
                    )

                    print(
                        "Offsets were NOT committed."
                    )

            except Exception as error:

                print(
                    f"Final upload error: {error}"
                )

                print(
                    "Offsets were NOT committed."
                )


        consumer.close()

        print()
        print("S3 consumer stopped.")


# ============================================================
# START APPLICATION
# ============================================================

if __name__ == "__main__":
    main()

