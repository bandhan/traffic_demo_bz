#! /bin/bash
sudo apt update
sudo apt install python python-dev python3 python3-dev
sudo apt install python-dev
sudo apt install -y build-essential libssl-dev libffi-dev python-dev

wget https://bootstrap.pypa.io/get-pip.py
sudo python get-pip.py
sudo pip install "google-cloud==0.19"
sudo pip install sodapy
sudo pip install --upgrade google-cloud-core
sudo pip install --upgrade google-cloud-pubsub google-cloud-storage

mkdir /home/bandhan/code
gsutil cp gs://chicago_traffic_gcp_demo_bandhan/src/stream_data.py /tmp/stream_data.py
sudo mv /tmp/stream_data.py /home/bandhan/code/stream_data.py

gsutil cp gs://chicago_traffic_gcp_demo_bandhan/src/data.json /tmp/data.json
sudo mv /tmp/data.json /home/bandhan/code/data.json

gsutil cp gs://chicago_traffic_gcp_demo_bandhan/src/update_data.py /tmp/update_data.py
sudo mv /tmp/update_data.py /home/bandhan/code/update_data.py

sudo python /home/bandhan/code/update_data.py

## Ignore for now
#sudo chown bandhan /home/bandhan/code
#sudo chown bandhan /home/bandhan/code
#sudo chown bandhan /home/bandhan/code/update_data.py
#sudo chgrp bandhan /home/bandhan/code

gsutil cp gs://chicago_traffic_gcp_demo_bandhan/src/update_data_scheduled /tmp/update_data_scheduled
sudo mv /tmp/update_data_scheduled /etc/cron.d/update_data_scheduled

sudo service cron restart
