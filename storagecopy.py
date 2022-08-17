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
SOURCE_URI = "https://segyshare.file.core.windows.net/largedata/zgy/psdn11_TbsdmF_Far_Nov_11_8bit.zgy?sv=2021-06-08&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2022-08-17T21:44:08Z&st=2022-08-17T13:44:08Z&spr=https&sig=%2FQKY3X1jXlOoo2FtR4ZpntWFqL7fE4a%2FpEkug3XhcmQ%3D"


# Identify the EXACT blob to create in the destination
BLOB_DESTINATION_URI = "https://3rvnd4iqs6x4y.blob.core.windows.net/testcontainer/test_norway.zip?sp=racwdli&st=2022-08-17T14:34:18Z&se=2022-08-17T22:34:18Z&spr=https&sv=2021-06-08&sr=c&sig=1QpLyd9Hiu%2B4YGyygaL7%2BNEFgpoZaL8WH%2BSZsVwuBnQ%3D"
# Identify only the folder to move, default file name is "0"
CONTAINER_DESTINATION_URI = "https://3rvnd4iqs6x4y.blob.core.windows.net/testcontainer?sp=racwdli&st=2022-08-17T14:34:18Z&se=2022-08-17T22:34:18Z&spr=https&sv=2021-06-08&sr=c&sig=1QpLyd9Hiu%2B4YGyygaL7%2BNEFgpoZaL8WH%2BSZsVwuBnQ%3D"

SOURCES = [
    SOURCE_URI,
    "Y:\\segy\\norway.zip"
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
