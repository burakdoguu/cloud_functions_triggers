import base64
import functions_framework
import json
from google.cloud import storage
import datetime

dt = datetime.datetime.now()

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    # Print out the data from Pub/Sub, to prove that it worked
    pubsub_message = base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8')
    print(pubsub_message)

    ## Cloud Storage
    storage_client = storage.Client()

    bucket_name = "retail_test_data"
    file_name = f"views_data-{dt.day}-{dt.month}-{dt.year}.txt"
    print(f"message insert into {file_name}")
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    if blob.exists():
        current_content = blob.download_as_text()
        new_content = current_content + '\n' + pubsub_message
    else:
        new_content = pubsub_message

    blob.upload_from_string(new_content)