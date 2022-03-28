from google.cloud import storage
from os import environ
import pandas as pd
import pandas_gbq
import re

environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""  # add location to service account


def ingest_data(data, context):
    """
    This function grabs files in a predefined directory on google cloud
    storage, it will search for (.CSV) files specifically and then
    transform it to prepare it for uploading into our Bigquery instance.

    Upload Location: gs://sfl_temp/File_Upload
    Bigquery location: `alfonzos-things-339215.SFL.SFL_Temp`

    Author: Alfonzo Sanfilippo <Alfonzo.Sanfilippo@gmail.com>

    This function was designed for google cloud functions
    --Must pass (data, context) in the dashboard

    :return: None
    """
    # preset
    bucket_Loc = "sfl_temp"
    path = "File_Upload"
    project_id = "alfonzos-things-339215"
    bq_loc = "SFL.SFL_Temp"

    # initialize Cloud Storage & Bigquery Clients
    storage_client = storage.Client()

    # Dev tool
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    for blob in storage_client.list_blobs(bucket_Loc, prefix=path):

        # search for .CSV file
        match = re.search("/(.+?).csv", str(blob))

        # when match read it into a pandas table
        if match:
            dataframe = pd.read_csv(f'gs://{bucket_Loc}/{path}/' + match.group(1) + ".csv", header=0, dtype=str)
            # add internal ID - {id}{first_initial}{last_initial}
            dataframe['internal_id'] = dataframe['id'] + dataframe['first_name'].str.get(0) + \
                                       dataframe['last_name'].str[0]
            # upload file to BQ
            pandas_gbq.to_gbq(dataframe, bq_loc, project_id=project_id, if_exists='append')

            # Move file in dir
            found = match.group(1) + ".csv"
            source_bucket = storage_client.get_bucket(bucket_Loc)
            source_blob = source_bucket.blob(f"{path}/" + found)
            destination_bucket = storage_client.get_bucket(bucket_Loc)
            source_bucket.copy_blob(source_blob, destination_bucket, "File_Completed/" + found)
            source_blob.delete()


ingest_data(None, None)
