import os
from datetime import datetime, timedelta

# from azure.storage.blob import BlockBlobService
# from azure.core.exceptions import ResourceExistsError

BLOB_CONN_STR = 'DefaultEndpointsProtocol=https;AccountName=dgm10050420storacct1;AccountKey=dsRII78ueNZba9XdVPq2Wfsr+vQVzksRYOFxcOhmvvHViwIvuG85zlE5vbnGbUTB4qmWHI7jGXk7hd8ZGv7Xuw==;EndpointSuffix=core.windows.net'

TARG_ROOT_FOLDER = r'C:\CompaniesSourceFilesReceived'
KROGERSHIP_SRC_FOLDER = 'kroger-ship'
KROGERSHIP_TARG_FOLDER = 'KrogerShip'


class ContainerSamples(object):

    # connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    connection_string = BLOB_CONN_STR

    def list_blobs_in_container(self):

        # Instantiate a BlobServiceClient using a connection string
        from azure.storage.blob import BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)

        # block_blob_service=BlockBlobService(account_name='dgm10050420storacct1',account_key='dsRII78ueNZba9XdVPq2Wfsr+vQVzksRYOFxcOhmvvHViwIvuG85zlE5vbnGbUTB4qmWHI7jGXk7hd8ZGv7Xuw==')

        # Instantiate a ContainerClient
        container_name = KROGERSHIP_SRC_FOLDER
        targ_folder = KROGERSHIP_TARG_FOLDER
        container_client = blob_service_client.get_container_client(container_name)

        blobs_list = container_client.list_blobs()
        blob_count = 0
        print('Container: ' + container_name)
        for blob in blobs_list:
            blob_count += 1
            print('Blob #: ' + str(blob_count))
            print('Name: ' + blob.name)
            blob_client = blob_service_client.get_blob_client(
                container=container_name, blob=blob)
            target_path = os.path.join(TARG_ROOT_FOLDER, targ_folder, blob.name)
            print('Target path: ' + target_path)
            # with open(target_path, "wb") as my_blob:
            #     my_blob.writelines([blob_client.download_blob().readall()])  
            # block_blob_service.get_blob_to_path(container,blob.name,target_path)
        # [END list_blobs_in_container]


if __name__ == '__main__':
    sample = ContainerSamples()
    # sample.container_sample()
    # sample.acquire_lease_on_container()
    # sample.set_metadata_on_container()
    # sample.container_access_policy()
    sample.list_blobs_in_container()
    # sample.get_blob_client_from_container()