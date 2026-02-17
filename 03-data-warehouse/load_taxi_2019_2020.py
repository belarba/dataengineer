import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time


BUCKET_NAME = "zoomcamp-487311-hw3"

CREDENTIALS_FILE = "gcs.json"
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

DOWNLOAD_DIR = "."
CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

# Generate all file combos: yellow + green, 2019 + 2020, months 01-12
FILES = []
for taxi_type in ["yellow", "green"]:
    for year in [2019, 2020]:
        for month in range(1, 13):
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            FILES.append(filename)


def download_file(filename):
    url = f"{BASE_URL}{filename}"
    file_path = os.path.join(DOWNLOAD_DIR, filename)

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def create_bucket(bucket_name):
    try:
        client.get_bucket(bucket_name)
        project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
        if bucket_name in project_bucket_ids:
            print(f"Bucket '{bucket_name}' exists. Proceeding...")
        else:
            print(f"Bucket '{bucket_name}' exists but does not belong to your project.")
            sys.exit(1)
    except NotFound:
        client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        print(f"Bucket '{bucket_name}' is not accessible. Try a different name.")
        sys.exit(1)


def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    if file_path is None:
        return

    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                # Remove local file after successful upload
                os.remove(file_path)
                print(f"Removed local file: {file_path}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")


if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    print(f"Total files to process: {len(FILES)}")

    # Download and upload in batches to avoid filling disk
    for filename in FILES:
        file_path = download_file(filename)
        if file_path:
            upload_to_gcs(file_path)

    print("All files processed and verified.")
