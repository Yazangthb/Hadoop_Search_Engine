#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt  

# Package the virtual env.
venv-pack -o .venv.tar.gz

# Collect data
bash prepare_data.sh


# Run the indexer
# bash index.sh data/sample.txt



# bash index.sh "data/460442_A_Bug's_Life.txt"
python3 preprocess_data.py "data/67227_A_Brief_History_of_Time.txt" "data/processed.txt"

# Index the processed TSV
bash index.sh "data/processed.txt"
python3 preprocess_data.py "data/73850178_A_Blazing_Grace.txt" "data/processed2.txt"

# Index the processed TSV
bash index.sh "data/processed2.txt"
python3 preprocess_data.py "data/73704692_A_Critical_Introduction_to_Skepticism.txt" "data/processed3.txt"

# Index the processed TSV
bash index.sh "data/processed3.txt"
# Run the ranker
bash search.sh "History"
bash search.sh "Skepticism"
bash search.sh "Grace"