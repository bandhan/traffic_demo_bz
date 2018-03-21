#!/usr/bin/python
from google.cloud import pubsub_v1, storage
from datetime import datetime, timedelta
from sodapy import Socrata
import os, io, json, urllib2

def publish_messages(project, topic_name, msg):
    """Publishes multiple messages to a Pub/Sub topic."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, topic_name)
    #msg = u'{"ID":"10","CITY":"BOSTON","SEGMENT_ID":"1","STREET":"test","DIRECTION":"test","FROM_STREET":"test","TO_STREET":"test","DISTANCE":"10","START_LONGTITUDE":"-1","START_LATITUTDE":"-1","END_LONGTITUDE":"1","END_LATITUTDE":"1","SPEED":"50","LAST_UPDATED":"2004-05-23T14:25:10.487","COMMENTS":"this is a test"}\n{"ID":"20","CITY":"CAMBRIDGE","SEGMENT_ID":"1","STREET":"test","DIRECTION":"test","FROM_STREET":"test","TO_STREET":"test","DISTANCE":"10","START_LONGTITUDE":"-1","START_LATITUTDE":"-1","END_LONGTITUDE":"1","END_LATITUTDE":"1","SPEED":"50","LAST_UPDATED":"2004-05-23T14:25:10.487","COMMENTS":"this is a test"}\n{"ID":"30","CITY":"ROCKWOOD","SEGMENT_ID":"1","STREET":"test","DIRECTION":"test","FROM_STREET":"test","TO_STREET":"test","DISTANCE":"10","START_LONGTITUDE":"-1","START_LATITUTDE":"-1","END_LONGTITUDE":"1","END_LATITUTDE":"1","SPEED":"50","LAST_UPDATED":"","COMMENTS":"this is a test"}'
    data = msg.encode('utf-8')
    publisher.publish(topic_path, data=data)
    
socrata_domain = 'data.cityofchicago.org'
socrata_dataset_identifier = 'n4j6-wkkf'

socrata_token = os.environ.get('SODAPY_APPTOKEN')

client = Socrata(socrata_domain, 'zGZx0sjX8p8VvQs4rs3FBZ1fx')

metadata = client.get_metadata(socrata_dataset_identifier)
last_updated = metadata.get('rowsUpdatedAt')


#load old results
filename = '/home/bandhan/code/data.json'
with open(filename, 'r') as f:
    current_data = json.load(f)

#date_diff= unicode(datetime.today() - timedelta(hours=5,minutes=25))   
#print (datetime.today())
#print (date_diff)

# Use the 'where' argument to filter the data before downloading it
results = client.get(socrata_dataset_identifier, order="segmentid ASC", limit=3000) # default limit is 1k

for i in range(len(results)):
    #print('OLD Data: ' + current_data[i].get('segmentid') + ' : ' + current_data[i].get('_last_updt'))
    #print('NEW Data: ' + results[i].get('segmentid') + ' : ' + results[i].get('_last_updt'))
    
    if results[i].get('_last_updt') > current_data[i].get('_last_updt'):
    	# print('TRUE') - we only care about this data, as other rows have not been updated in 3 days
		print (results[i].get('segmentid') + ' (new speed):' + results[i].get('_traffic') + ' recorded at ' + results[i].get('_last_updt'))
		time = results[i].get('_last_updt').encode("ascii")
		segmentID = results[i].get('segmentid').encode("ascii")
		street = results[i].get('street').encode("ascii")
		fromSt = results[i].get('_fromst').encode("ascii")
		toSt = results[i].get('_tost').encode("ascii")
		direction = results[i].get('_direction').encode("ascii")
		strHeading = results[i].get('_strheading').encode("ascii")
		speed = results[i].get('_traffic').encode("ascii")
		length = results[i].get('_length').encode("ascii")
		startLat = results[i].get('_lif_lat').encode("ascii")
		startLong = results[i].get('start_lon').encode("ascii")
		endLat = results[i].get('_lit_lat').encode("ascii")
		endLong = results[i].get('_lit_lon').encode("ascii")
		msg = ('{\"Time\":\"%s\",\"SegmentID\":\"%s\",\"Street\":\"%s\",\"FromSt\":\"%s\",\"ToSt\":\"%s\",\"Direction\":\"%s\",\"StrHeading\":\"%s\",\"Speed\":\"%s\",\"Length\":\"%s\",\"StartLat\":\"%s\",\"StartLong\":\"%s\",\"EndLat\":\"%s\",\"EndLong\":\"%s\"}' % (time, segmentID, street, fromSt, toSt, direction, strHeading, speed, length, startLat, startLong, endLat, endLong))
		publish_messages('chicago-traffic-gcp-demo','chicago_traffic_rt', msg)
        
  #              print(i)
  #              print len(results)
  #              print len(current_data)
                #print (results[i])
                #print (current_data[i])
#print("Number of results downloaded: {}".format(len(results)))


with io.open('/home/bandhan/code/data.json', 'w', encoding='utf-8') as f:
  f.write(json.dumps(results, indent=3, ensure_ascii=False))