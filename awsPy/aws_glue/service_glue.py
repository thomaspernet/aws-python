import boto3, os
#from sagemaker import get_execution_role
#import dask.dataframe as dd

class connect_glue():
    def __init__(self, client = None, bucket=None, credentials = None):
        """
        crediitnals is a list
        """
        self.client =client
        self.bucket = bucket

    def get_table_information(self, database, table):
        """
        database: Database name
        table: Table name

        """
        response = self.client['glue'].get_table(
        DatabaseName=database,
        Name=table
    )
        return response

    def update_schema_table(self, database, table, schema):
        """
        database: Database name
        table: Table name
        schema: a list of dict:
        [
        {
        'Name': 'geocode4_corr',
        'Type': '',
        'Comment': 'Official chinese city ID'}
        ]
        """

        response = self.client['glue'].get_table(
        DatabaseName=database,
        Name=table
    )
        list_schema = response['Table']['StorageDescriptor']['Columns']
        for field in list_schema:
            try:
                field['Comment'] = next(
                    item for item in schema if item["Name"] == field['Name']
                )['Comment']

            except:
                pass

        self.client['glue'].update_table(
            DatabaseName=database,
            TableInput = {
                'Name':table,
                'StorageDescriptor':{
                    'Columns' : list_schema
                }
            }
        )

        return list_schema
