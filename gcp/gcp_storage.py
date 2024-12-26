import logging

from google.cloud import storage


class ManagerGCP:

    def __init__(self):
        self.storage_client = storage.Client()

    def upload_to_gcs(self, bucket_name, file, file_name, content_type):
        """Uploads a file to the bucket."""
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(file_name)
            blob.upload_from_file(file, content_type=content_type)
            # blob.make_public()

        except Exception:
            logging.error(f'GCPFactory - Error checking permission: {file_name}')
            raise
