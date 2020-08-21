import pandas as pd
import os, boto3, json

class aws_instantiate():
    def __init__(self, credential, region):
        self.credential = credential
        self.region= region

#### LOAD CREDENTIAL
    def load_credential(self):
        """
        """
        if type(self.credential) is not list:
            filename, file_extension = os.path.splitext(self.credential)
            if file_extension== '.csv':
                load_cred = pd.read_csv(self.credential)

                key = load_cred.iloc[0, 2]
                secret_ = load_cred.iloc[0, 3]

            if file_extension== '.json':
                with open(self.credential) as json_file:
                    data = json.load(json_file)

                    key = data['aws_access_key_id']
                    secret_ = data['aws_secret_access_key']
        else:
            key = self.credential[0]
            secret_ = self.credential[1]
        return key, secret_

    def client_boto(self):
        """
        option -> ['s3', 'athena', 'ses', 'sns', 'resource', 'logs']
        temporary, we load all clients...
        """

        key, secret_ = self.load_credential()
        store_client = {
        #'service': None
        }
        for i, option in enumerate(['s3', 'athena', 'ses', 'sns', 'resource', 'logs']):
            if option == 'resource':
                client_ = boto3.resource(
                    's3',
                    aws_access_key_id=key,
                    aws_secret_access_key=secret_,
                    region_name = self.region
                    )

                #client = {
                #'option': option,
                #'client': client_
                #}

            else:
                client_ = boto3.client(
                option,
                aws_access_key_id=key,
                aws_secret_access_key=secret_,
                region_name = self.region
                )
                #client = {
                #'option': option,
                #'client': client_
                #}
            store_client[option] = client_

        return store_client
