#!/usr/bin/python
from google.cloud import pubsub_v1, storage
from datetime import datetime, timedelta
from sodapy import Socrata
import os, io, json, urllib2

def publish_messages(project, topic_name, msg):
    """Publishes multiple messages to a Pub/Sub topic."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, topic_name)
    msg = u'{"ID":"10","CITY":"BOSTON","SEGMENT_ID":"1","STREET":"test","DIRECTION":"test","FROM_STREET":"test","TO_STREET":"test","DISTANCE":"10","START_LONGTITUDE":"-1","START_LATITUTDE":"-1","END_LONGTITUDE":"1","END_LATITUTDE":"1","SPEED":"50","LAST_UPDATED":"2004-05-23T14:25:10.487","COMMENTS":"this is a test"}\n{"ID":"20","CITY":"CAMBRIDGE","SEGMENT_ID":"1","STREET":"test","DIRECTION":"test","FROM_STREET":"test","TO_STREET":"test","DISTANCE":"10","START_LONGTITUDE":"-1","START_LATITUTDE":"-1","END_LONGTITUDE":"1","END_LATITUTDE":"1","SPEED":"50","LAST_UPDATED":"2004-05-23T14:25:10.487","COMMENTS":"this is a test"}\n{"ID":"30","CITY":"ROCKWOOD","SEGMENT_ID":"1","STREET":"test","DIRECTION":"test","FROM_STREET":"test","TO_STREET":"test","DISTANCE":"10","START_LONGTITUDE":"-1","START_LATITUTDE":"-1","END_LONGTITUDE":"1","END_LATITUTDE":"1","SPEED":"50","LAST_UPDATED":"","COMMENTS":"this is a test"}'
    data = msg.encode('utf-8')
    publisher.publish(topic_path, data=data)
    
socrata_domain = 'data.cityofchicago.org'
socrata_dataset_identifier = 'n4j6-wkkf'

# If you choose to use a token, run the following command on the terminal (or add it to your .bashrc)
# $ export SODAPY_APPTOKEN=<token>
socrata_token = os.environ.get('SODAPY_APPTOKEN')

client = Socrata(socrata_domain, socrata_token)

metadata = client.get_metadata(socrata_dataset_identifier)
last_updated = metadata.get('rowsUpdatedAt')


#load old results
filename = 'data.json'
with open(filename, 'r') as f:
    current_data = json.load(f)

#date_diff= unicode(datetime.today() - timedelta(hours=5,minutes=25))   
#print (datetime.today())
#print (date_diff)

# Use the 'order' argument to order the data before downloading it
results = client.get(socrata_dataset_identifier, order="segmentid ASC", limit=2000) # default limit is 1k

for i in range(len(results)):
    #print('OLD Data: ' + current_data[i].get('segmentid') + ' : ' + current_data[i].get('_last_updt'))
    #print('NEW Data: ' + results[i].get('segmentid') + ' : ' + results[i].get('_last_updt'))
    if results[i].get('_last_updt') > current_data[i].get('_last_updt'):
                # print('TRUE') - we only care about this data, as other rows have not been updated in 3 days
                # do something w/ the new/updated data
                #print(results[i].get('segmentid') + ' (new speed):' + results[i].get('_traffic') + ' recorded at ' + results[i].get('_last_updt'))
                publish_messages('chicago-traffic-gcp-demo','chicago_traffic_rt', results[i])
                #print (results[i])
                #print (current_data[i])

#write to file (for caching latest results and checking in future whether there is an update)
with io.open('data.json', 'w', encoding='utf-8') as f:
  f.write(json.dumps(results, indent=3, ensure_ascii=False))