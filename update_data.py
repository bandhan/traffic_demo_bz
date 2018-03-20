#!/usr/bin/python

from google.cloud import storage
import urllib2
import json
response = urllib2.urlopen('https://data.cityofchicago.org/resource/8v9j-bter.json')
data = json.load(response)   


#Output for debugging:
#print data


#Write to file for storage in Cloud Storage:
with open('data.json', 'w') as outfile:
    json.dump(data, outfile)

#Function to upload file to Google Cloud Storage bucket:

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print('File {} uploaded to {}.'.format(
    source_file_name,
    destination_blob_name))

upload_blob("chicago_traffic_gcp_demo_bandhan","data.json","data.json")