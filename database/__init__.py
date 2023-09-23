import logging
import os

from pymongo import MongoClient
from pymongo.server_api import ServerApi

CONNECTION_STRING = "mongodb+srv://estore-east-1.lbomhi6.mongodb.net/?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority"

logging.basicConfig(level=logging.INFO)


CERTIFICATE_PATH = os.path.abspath('database/certificates/X509-cert-faizan-estore.pem')
def connection_status():
    # Set the Stable API version when creating a new client
    client = MongoClient(CONNECTION_STRING,
                         tls=True,
                         tlsCertificateKeyFile=CERTIFICATE_PATH,
                         server_api=ServerApi('1'))
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
        return True
    except Exception as e:
        print(e)
        return False


def mongo_client():
    try:
        client = MongoClient(CONNECTION_STRING,
                             tls=True,
                             tlsCertificateKeyFile=CERTIFICATE_PATH,
                             server_api=ServerApi('1'))
        logging.debug(f"Created connection to MongoDB {client.server_info()}")
        return client
    except Exception as e:
        print(e)
        logging.warning(f"Failed to create connection to MongoDB {e}")
        return False


connection_status()
mongo_client()
