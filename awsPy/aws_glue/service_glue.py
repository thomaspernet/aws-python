import boto3, os
#from sagemaker import get_execution_role
#import dask.dataframe as dd

class connect_glue():
    def __init__(self, client = None, bucket=None, credentials = None):
        """
        crediitnals is a list
        """
        self.client =client

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
        key_to_remove = ['DatabaseName', 'CreateTime', 'UpdateTime', 'CreatedBy',
                 'IsRegisteredWithLakeFormation', 'CatalogId']

        response = self.client['glue'].get_table(
        DatabaseName=database,
        Name=table
    )['Table']
        #### update the schema
        list_schema = response['StorageDescriptor']['Columns']
        for field in list_schema:
            try:
                ## Update type
                field['Type'] = next(
                    item for item in schema if item["Name"] == field['Name']
                )['Type']

                ## Update Comment

                field['Comment'] = next(
                    item for item in schema if item["Name"] == field['Name']
                )['Comment']

            except:
                pass

        response['StorageDescriptor']['Columns'] = list_schema
        for key in key_to_remove:
            response.pop(key, None)

        update = self.client['glue'].update_table(
            DatabaseName=database,
            TableInput = response
        )

        return update
