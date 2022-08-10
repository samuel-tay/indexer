# EVM Indexer
This repository hosts the code that was written as part of the paper `Decentralised Finance: A study into hacks and how to participate safely` written by Samuel Tay (100993275).

# Usage
A Python >3.8 environment is required.

Install the libraries needed using `pip3 install -r requirements.txt`.

The `config.py` file should be filled in with an Archive Node and a random string for the flask secret.

`indexer.py` is used to index and build the database. It is run using `python3 indexer.py`.

`webapp.py` is used to run the webserver for the web app. Run `python3 webapp.py` t is then accessed using `http://127.0.0.1:5000`.
