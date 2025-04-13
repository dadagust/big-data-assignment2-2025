#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

echo "Creating"
python3 -m venv .venv
echo "Activating"
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt  

echo "Packing"
venv-pack -o .venv.tar.gz

# Collect data
bash prepare_data.sh

echo "INDEXING DATA"
bash index.sh /data


# Run the ranker
bash search.sh "this is a query!"