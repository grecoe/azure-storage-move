#######################################################################
# Use the BlobUri class to validate information about an Azure Storage
# SAS tokenized URI to a container or blob. 
#


from utils.identifiers import BlobUri
from urllib import parse

user_delegated_sas = "https://makebelieve.blob.core.windows.net/container_name/blob_name.txt?sv=2021-06-08&&se=2022-08-24T21:07:20Z&ske=2023-08-24T17:07:20Z&skt=2022-08-19T13:07:20Z&spr=https&sig=NO_SIGNATURE_HERE"
portal_sas = "https://makebelieve.blob.core.windows.net/container_name/blob_name.txt?sv=2021-06-08&&se=2023-08-24T21:07:20Z&st=2022-08-19T13:07:20Z&spr=https&sig=NO_SIGNATURE_HERE"
portal_container_sas = "https://makebelieve.blob.core.windows.net/container_name/?sv=2021-06-08&&se=2023-08-24T21:07:20Z&st=2022-08-19T13:07:20Z&spr=https&sig=NO_SIGNATURE_HERE"

sas_collection = {
    "delegated" : BlobUri(user_delegated_sas),
    "portal" : BlobUri(portal_sas),
    "container" : BlobUri(portal_container_sas)
}

for sas in sas_collection:
    current = sas_collection[sas]
    print("SAS:", sas)
    print("\tiscontainer: ", current.is_container())
    print("\tdelgated:", current.is_user_delegated_sas())
    print("\tstill valid:", current.get_remaining_time() > 0)
    print("\ttime window:")
    times = current.get_time_window()
    for t in times:
        print("\t\t{} = {}".format(t, times[t]))
