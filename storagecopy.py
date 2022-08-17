#######################################################################
# Copy a blob from one location to another using the azure.storage.blob
# python libraries.
#
#
# Input to the call are two SAS tokenized URI's. The first being the 
# input file, the file to be copied. The second the SAS tokenized URI
# for a file we WANT to create.

from utils.copyutil import(
    FilePath,
    BlobUri,
    BlobCopyUtil
)

# Source URI is storage file we want to move to another storage location 
SOURCE_URI = "https://ACCOUNT.file.core.windows.net/CONTAINER/PATH1/EXISTING_FILE?SAS_TOKEN"


# Identify the EXACT blob to create in the destination
BLOB_DESTINATION_URI = "https://ACCOUNT.file.core.windows.net/CONTAINER/PATH1/FILE_TO_CREATE?SAS_TOKEN"
# Identify only the folder to move, default file name is "0"
CONTAINER_DESTINATION_URI = "https://ACCOUNT.file.core.windows.net/CONTAINER?SAS_TOKEN"

SOURCES = [
    SOURCE_URI,
    "path\\to\\local\\file"
]
SOURCE_SELECTOR = 0

DESTINATIONS = [
    BLOB_DESTINATION_URI, 
    CONTAINER_DESTINATION_URI
]
DESTINATION_SELECTOR = 0



##############################################################
# With a destination URI and a source local location or 
# URI you can accomplish a copy in a few lines of code
##############################################################


# Destination URI can be either a full blob or container
destination_uri = BlobUri(DESTINATIONS[DESTINATION_SELECTOR])
# Source can be either a local file or a SAS tokenized blob URI
source_location = SOURCES[SOURCE_SELECTOR]

source = None
try:
    # Throws a ValueError if not a valid SAS tokenized blob URI
    source = BlobUri(source_location)
except ValueError as ve:
    # If this also throws a ValueError it's totally invalid
    source = FilePath(source_location)

copy_util = BlobCopyUtil(source, destination_uri) 
copy_util.copy_to_blob()
