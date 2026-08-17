import boto3
from pathlib import Path
from botocore.exceptions import ClientError


# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent

HISTORICAL_DIR = BASE_DIR / "data" / "historical"

BUCKET_NAME = "de-streaming-json-lake-2026-480749289287-ap-south-1-an"

S3_PREFIX = "historical/retail_events"


# ============================================================
# AWS S3 CLIENT
# ============================================================

s3 = boto3.client("s3")


# ============================================================
# UPLOAD FUNCTION
# ============================================================

def upload_file(file_path):

    relative_path = file_path.relative_to(HISTORICAL_DIR)

    s3_key = (
        f"{S3_PREFIX}/"
        f"{relative_path.as_posix()}"
    )

    try:

        s3.upload_file(
            str(file_path),
            BUCKET_NAME,
            s3_key
        )

        print(
            f"Uploaded: {s3_key}"
        )

    except ClientError as e:

        print(
            f"ERROR uploading {file_path}: {e}"
        )


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 60)
    print("HISTORICAL RETAIL DATA → S3")
    print("=" * 60)

    if not HISTORICAL_DIR.exists():

        print(
            f"Historical directory not found: "
            f"{HISTORICAL_DIR}"
        )

        return

    json_files = list(
        HISTORICAL_DIR.rglob("*.json")
    )

    print(
        f"Found {len(json_files):,} JSON files."
    )

    print(
        f"Uploading to s3://{BUCKET_NAME}/{S3_PREFIX}/"
    )

    print()

    for index, file_path in enumerate(
        json_files,
        start=1
    ):

        upload_file(file_path)

        if index % 100 == 0:

            print(
                f"Progress: "
                f"{index:,}/{len(json_files):,}"
            )

    print()
    print("Historical upload completed.")


if __name__ == "__main__":
    main()