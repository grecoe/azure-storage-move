"""
Identifiers are used to determing the inputs for both source and destination.

Source can be either a FilePath or BlobUri, but destination must be a BlobUri
"""
import os
from datetime import datetime

class FilePath:
    def __init__(self, file_name:str):

        if not os.path.exists(file_name):
            raise ValueError("File {} does not exist.".format(file_name))

        self.file_name = file_name
        self.file_size = os.path.getsize(self.file_name)

class BlobUri:
    """
    Azure Blob URI parser. Used to determine if we have a valid URI and that it's
    SAS token is not yet expired. 

    Used to copy from one location to another
    """
    def __init__(self, uri:str):
        self.uri:str = uri
        self.storage_account:str = None
        self.container:str = None
        self.blob_path:str = None
        self.sas_active_from:datetime = None
        self.sas_active_to:datetime = None

        if "?" not in self.uri:
            raise ValueError("URI must contain SAS token")

        self._parse_account()
        self._parse_token()

        if self.get_remaining_time() <= 0:
            raise TimeoutError("Token expired for : {}".format(self.uri))

    def is_container(self):
        return len(self.blob_path) == 0

    def get_remaining_time(self) -> int:
        """
        Get remaining seconds left on token
        """
        current = datetime.utcnow()
        return int((self.sas_active_to - current).total_seconds())

    def _parse_account(self):
        """
        Break down the account/blob information into discrete pieces.
        """
        account_info = self.uri.split("?")[0]
        account_info = account_info.split("//")[1]
        account_info = account_info.split("/")

        # Have the account broken into parts
        self.storage_account = account_info[0].split(".")[0]
        self.container = account_info[1]
        self.blob_path = "/".join(account_info[2:])

    def _parse_token(self):
        """
        Break down the SAS token to get start/end times.
        """
        token_info = self.uri.split("?")[1]
        token_info = token_info.split("&")

        start = [x for x in token_info if x.startswith("st=")]
        end = [x for x in token_info if x.startswith("se=")]

        if len(start) == 0:
            raise ValueError("Invalid SAS token start time")
        if len(end) == 0:
            raise ValueError("Invalid SAS token end time")

        self.sas_active_from = datetime.strptime(
            start[0].split("=")[1],
            '%Y-%m-%dT%H:%M:%SZ'
        )

        self.sas_active_to = datetime.strptime(
            end[0].split("=")[1],
            '%Y-%m-%dT%H:%M:%SZ'
        )
