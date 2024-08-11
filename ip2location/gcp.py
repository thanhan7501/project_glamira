from google.cloud import storage


def upload_blob_from_stream(bucket_name, data, destination_blob_name, project_id):
    """Uploads bytes from a stream or other file-like object to a blob."""
    # Construct a client-side representation of the blob.
    storage_client = storage.Client(project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Upload data from the stream to your bucket.
    blob.upload_from_file(data, 'application/jsonl')
    
    print(
        f"Stream data uploaded to {destination_blob_name} in bucket {bucket_name}."
    )