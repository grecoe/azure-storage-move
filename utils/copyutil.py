"""
Copy util used for moving files to Azure Storage
"""


from email.policy import default
import typing
import time
import sys
from datetime import datetime
from azure.storage.blob import BlobClient, ContainerClient
from azure.core.exceptions import ClientAuthenticationError

from utils.identifiers import FilePath, BlobUri


class _BlobCopyStatus:
    """
    Utility class to capture information from each iteration 
    of getting the blob copy propertis. 
    """
    def __init__(self, status, progress):
        self.recorded:datetime = datetime.now()
        self.status:str = status
        sizes = progress.split("/")

        self.moved = int(sizes[0])
        self.total = int(sizes[1])
        self.percentage = float((self.moved/self.total * 100))
    
    def report(self, copy_status:list):
        """
        Print out information about this current status.
        """
        rate = 0.0
        if len(copy_status) >= 2:
            # This instance is last
            rate = self._get_deltas(copy_status[-2])

        print("{} - {} of {} - {}% - {} mb/s".format(
            self.status,
            self.moved,
            self.total,
            "{0:.2f}".format(self.percentage),
            "{0:.2f}".format(rate)
        ))

    def _get_deltas(self, copy_status):
        """
        Called only when there is another BlobCopyStatus to determine 
        how much data has moved between the two times and record the 
        actual data speed. 
        """
        delta = (self.recorded - copy_status.recorded).total_seconds()
        multiplier = int(60/delta)

        total = self.moved - copy_status.moved
        total_mb = float(total/(1024*1024))

        return (multiplier * total_mb) / 60

class BlobCopyUtil:
    """
    Utility used to copy one blob to another in Azure. 
    """
    copy_status:typing.List[_BlobCopyStatus] = []

    def __init__(self, source:typing.Union[BlobUri, FilePath], destination:BlobUri):
        self.source:typing.Union[BlobUri, FilePath] = source
        self.destination_stg:BlobUri = destination

        self.container_client:ContainerClient = None

    def copy_to_blob(self, scan_delay:int = 10, default_client = "0") -> bool:
        """
        Actual copy operation, it will (currently) print out it's status. 

        Parameters:
        scan_delay: Seconds between each recording of the state of the copy, only used 
            with storage to storage copy.
        default_client: Default blob name to create when copying local file to storage
            mimic what was already done in SDUTILS 
        """

        return_value = False
        BlobCopyUtil.copy_status = []

        # Make sure we aren't starting with an already expired token
        if self.destination_stg.get_remaining_time() <= 0:
            raise TimeoutError("Destination SAS token has timed out!")

        with self._get_blob_client(default_client=default_client) as blob_client:

            try:
                if isinstance(self.source, FilePath):
                    print("Copy Source is a file: {}".format(self.source.file_name))
                    return_value = self._disk_to_storage_copy(blob_client)
                elif isinstance(self.source, BlobUri):
                    print("Copy Source is a storage file:\n\t{}".format(self.source.uri))
                    return_value = self._storage_to_storage_copy(blob_client, scan_delay)
                else:
                    raise ValueError("Source is incompatible type.")
            except Exception as ex:
                print("Copy failed: ", str(ex))
            finally:
                if self.container_client:
                    self.container_client.close()
                    self.container_client = None

        return return_value


    def _disk_to_storage_copy(self, blob_client:BlobClient) -> bool:
        if not isinstance(self.source, FilePath):
            raise ValueError("_disk_to_storage_copy - source is not a FilePath")

        _test_start = datetime.now()
        start_time = time.time()

        with open(self.source.file_name, "rb") as lfile:
            print('- Initializing transfer session ... ')

            def callback(response):
                current = response.context['upload_stream_current']

                if current is not None:
                    total = response.context["data_stream_total"]
                    cur_status = _BlobCopyStatus("pending", "{}/{}".format(current, total))
                    BlobCopyUtil.copy_status.append(cur_status)
                    cur_status.report(BlobCopyUtil.copy_status)

            try:
                blob_client.upload_blob(lfile, validate_content=False,
                                        raw_response_hook=callback)
            except ClientAuthenticationError as cax:
                raise TimeoutError("SAS Token has expired.")

        lfile.close()
        ctime = time.time() - start_time + sys.float_info.epsilon
        speed = str(round(((self.source.file_size / 1048576.0) / ctime), 3))

        _test_delta = datetime.now() - _test_start

        print('- Transfer completed: ' + speed + ' [MB/s] in ', _test_delta.total_seconds(), "seconds")

        return True

    def _storage_to_storage_copy(self, blob_client:BlobClient, scan_delay:int = 10) -> bool:

        if not isinstance(self.source, BlobUri):
            raise ValueError("_storage_to_storage_copy - source is not a BlobUri")

        blob_client.start_copy_from_url(self.source.uri)
            
        complete:bool = False
            
        # Keep copying while SAS is valid
        start_copy = datetime.now()

        while self.destination_stg.get_remaining_time() > 0:
            props = blob_client.get_blob_properties()
            status = props.copy.status
                
            cur_status:_BlobCopyStatus = _BlobCopyStatus(status, props.copy.progress)
            BlobCopyUtil.copy_status.append(cur_status)

            cur_status.report(BlobCopyUtil.copy_status)

            if status == "success":
                complete = True
                break
                    
            time.sleep(scan_delay)
            
        print("Copy operation complete in {} seconds".format(
            (datetime.now() - start_copy).total_seconds()
        ))

        # If it didn't complete, report it. Raise on timeout only
        if not complete:
            last_status = "unknown"
            if len(BlobCopyUtil.copy_status) > 0:
                last_status = BlobCopyUtil.copy_status[-1].status

            message = "Last status: {} Remaining Time: {}".format(
                last_status,
                self.destination_stg.get_remaining_time()                    
            )
            print(message)

            if self.destination_stg.get_remaining_time() <= 0:
                raise TimeoutError("Destination SAS token has timed out!")

        return complete

    def _get_blob_client(self, default_client="0") -> BlobClient:
        """
        Get a suitable BlobClient for an upload to. 
        
        If the URI is for a storage container, use the default_client as the blob client (blob name). 

        If the URI is for a blob, use the supplied blob as the actual blob.
        """
        _maxBlockSize = 64 * 1024 * 1024
        _max_single_put_size = 64 * 1024 * 1024
        _max_concurrency = 10

        return_client:BlobClient = None

        if self.destination_stg.is_container():
            print("Destination is an Azure Storage Container URI\n\t{}".format(self.destination_stg.uri))
            self.container_client = ContainerClient.from_container_url(container_url=self.destination_stg.uri,
                                                    max_block_size=_maxBlockSize,
                                                    use_byte_buffer=True,
                                                    max_concurrency=_max_concurrency,
                                                    max_single_put_size=_max_single_put_size,
                                                    connection_timeout=100)

            print("Blob to create: {}".format(default_client))
            return_client = self.container_client.get_blob_client(default_client)
        else:
            print("Destination is an Azure Blob URI\n\t{}".format(self.destination_stg.uri))
            return_client = BlobClient.from_blob_url(self.destination_stg.uri)

        return return_client
