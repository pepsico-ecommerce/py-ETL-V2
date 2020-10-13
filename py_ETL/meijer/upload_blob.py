import os
from datetime import datetime, timedelta

#BLOB_CONN_STR = 'DefaultEndpointsProtocol=https;AccountName=dgm10050420storacct1;AccountKey=dsRII78ueNZba9XdVPq2Wfsr+vQVzksRYOFxcOhmvvHViwIvuG85zlE5vbnGbUTB4qmWHI7jGXk7hd8ZGv7Xuw==;EndpointSuffix=core.windows.net'
BLOB_CONN_STR = 'DefaultEndpointsProtocol=https;AccountName=dgm1005storacct;AccountKey=52SXW8QKiwFTFp53ILHoHMzA66moZVe8EyIcCIN2iZlwWWiXzbBnFKFAg049hxtCHhhgp+doALtJGDPrwAxnng==;EndpointSuffix=core.windows.net'

LOCAL_CSV_PATH = r'C:\CompaniesSourceFilesConvertedToCSV'
CUSTOMER_NAME = 'meijer'
CONTAINER_NAME = CUSTOMER_NAME


class MeijerUploadBlob:

    def __init__(self, F_PATH):
        self.F_PATH = F_PATH

    def perf_upload(self, debug):
        full_path_to_file = self.F_PATH
        local_file_name = os.path.basename(full_path_to_file)
        if (debug):
            print('full path: ' + full_path_to_file)
            print('local path: ' + local_file_name)

        # connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        connection_string = BLOB_CONN_STR

        # Instantiate a BlobServiceClient using a connection string
        from azure.storage.blob import BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Upload the created file, use local_file_name for the blob name
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME, blob=local_file_name)
        with open(full_path_to_file, "rb") as data:
            blob_client.upload_blob(data)
