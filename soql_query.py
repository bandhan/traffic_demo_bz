#!/usr/bin/python
import os, io, json, urllib2
from google.cloud import pubsub_v1, storage
from datetime import datetime, timedelta
from sodapy import Socrata

socrata_domain = 'data.cityofchicago.org'
socrata_dataset_identifier = 'n4j6-wkkf'

# If you choose to use a token, run the following command on the terminal (or add it to your .bashrc)
# $ export SODAPY_APPTOKEN=<token>
socrata_token = os.environ.get('SODAPY_APPTOKEN')

client = Socrata(socrata_domain, socrata_token)

metadata = client.get_metadata(socrata_dataset_identifier)
last_updated = metadata.get('rowsUpdatedAt')


date_diff= unicode(datetime.today() - timedelta(days=3))   

# Use the 'where' argument to filter the data before downloading it
results = client.get(socrata_dataset_identifier, limit=2000) # default limit is 1k
for i in range(len(results)):
	if results[i].get('_last_updt') >= date_diff:
		# print('TRUE') - we only care about this data, as other rows have not been updated in 3 days
		# do something
		


#print("Number of results downloaded: {}".format(len(results)))
#print json.dumps(results, indent=4)

# Publish message to pub/sub:
def publish_messages(project, topic_name):
    """Publishes multiple messages to a Pub/Sub topic."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, topic_name)

    for n in range(1, 10):
        data = u'Message number {}'.format(n)
        # Data must be a bytestring
        data = data.encode('utf-8')
        publisher.publish(topic_path, data=data)

    print('Published messages.')

publish_messages('chicago-traffic-gcp-demo','chicago_traffic_rt')

#### messages published

with io.open('data.json', 'w', encoding='utf-8') as f:
  f.write(json.dumps(results, indent=3, ensure_ascii=False))

# ### Restrict columns and order rows
# Often, you know which columns you want, so you can further simplify the download.
#
# It can also be valuable to have results in order, so that you can quickly grab the
# largest or smallest.

# In[8]:


#results = client.get(socrata_dataset_identifier,
#                     where="amount < 2433",
#                     select="amount, job",
#                    order="amount ASC")
#results[:3]




# ### Break download into managable chunks
# Sometimes you do want all the data, but it would be too big for one download.
#
# By default, all queries have a limit of 1000 rows, but you can manually set it
# higher or lower. If you want to loop through results, just use `offset`

# In[11]:


#results = client.get(socrata_dataset_identifier, limit=6, select="name, amount")
#results


# In[11]:

# 
# loop_size = 500
# num_loops = 4
# 
# for i in range(num_loops):
#     results = client.get(socrata_dataset_identifier,
#                          limit=loop_size,
#                          offset=loop_size * i)
#     print("\n> Loop number: {}".format(i))
# 
#     # This simply formats the output nicely
#     for result in results:
#         print(result)
        
# 
# 
# # ### Query strings
# # All of the queries above were made with method parameters,
# # but you could also pass all the parameters at once in a
# # SQL-like format
# 
# # In[13]:
# 
# 
# query = """
# select
#     name,
#     amount
# where
#     amount > 1000
#     and amount < 2000
# limit
#     5
# """
# 
# results = client.get(socrata_dataset_identifier, query=query)
# results



