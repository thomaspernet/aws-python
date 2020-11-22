import boto3, os, time
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

    def create_table_glue(self, target_S3URI, name_crawler, Role, DatabaseName,
    TablePrefix, from_athena=True, update_schema=None):
        """
        Create a table in Glue data catalog

        Args:

        - target_S3URI: string. S3 Location of the files to parse
        - name_crawler: string. Define a crawler name
        - Role: string. ARN role nam, ie. arn:aws:iam::468786073381:role/AWSGlueServiceRole-XXX
        - DatabaseName: string. Database name to add the new table
        - TablePrefix: string. Table prefix. Full name will be TablePrefix + folder name
        - from_athena;: Boolean. If True, then change the schema as follow:
            - Serde serialization lib: org.apache.hadoop.hive.serde2.OpenCSVSerde
            - Serde parameters
                - escapeChar: \\
                - quoteChar: "
                - separatorChar:,
                - serialization.format: 1
        - Schema: if not None then List. Update the schema in the catalog. Should be as follow:
        "schema":[
                   {
                      "Name":"",
                      "Type":"",
                      "Comment":""
                   }
                ]

        return Crawler jobs

        """

        # Get connection
        #client_glue = client['glue']

        table_name = '{}{}'.format(TablePrefix, os.path.basename(target_S3URI).lower())

        ## Remove table if exist
        try:
            response = self.client['glue'].delete_table(
            DatabaseName=DatabaseName,
            Name=table_name
        )
        except Exception as e:
            print(e)
            pass

        # Remove if exist
        try:
            self.client['glue'].delete_crawler(
            Name=name_crawler
        )
        except Exception as e:
            print(e)
            pass

        self.client['glue'].create_crawler(
            Name=name_crawler,
            Role=Role,
            DatabaseName=DatabaseName,
            #Description='Parse the symbols filtered by a given timestamp',
            Targets={
                'S3Targets': [
                    {
                        'Path': target_S3URI,
                        'Exclusions': [
                            '*.csv.metadata',
                        ]
                    },
                ],
            },
            TablePrefix=TablePrefix,
            SchemaChangePolicy={
                'UpdateBehavior': 'LOG',
                'DeleteBehavior': 'LOG'
            },
        )

        # Wait until job is done
        self.client['glue'].start_crawler(Name=name_crawler)
        status = None
        while status == 'RUNNING' or status == None:
            response_crawler = self.client['glue'].get_crawler(Name=name_crawler)
            status = response_crawler['Crawler']['State']
            if (status == 'RUNNING' or status == None):
                time.sleep(10)

        # Update Schema
        if from_athena:
            # update schema serde

            response = self.client['glue'].get_table(
                DatabaseName=DatabaseName,
                Name=table_name
            )['Table']

            serde = {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
                "Parameters": {
                    "escapeChar": "\\",
                    "quoteChar": '"',
                    "separatorChar": ",",
                    "serialization.format": "1",
                },
            }

            response['StorageDescriptor']['SerdeInfo'] = serde
            key_to_remove = ['DatabaseName', 'CreateTime', 'UpdateTime', 'CreatedBy',
                             'IsRegisteredWithLakeFormation', 'CatalogId']
            for key in key_to_remove:
                response.pop(key, None)

            self.client['glue'].update_table(
                DatabaseName=DatabaseName,
                TableInput=response
            )

        # Update schema

        if update_schema != None:

            self.update_schema_table(
                database=DatabaseName, table=table_name, schema=update_schema)

        response = self.client['glue'].get_table(
                DatabaseName=DatabaseName,
                Name=table_name
            )['Table']

        return response

    def delete_table(self, database, table):
        """
        Delete table

        database (string) --
        [REQUIRED]

        The name of the catalog database in which the table resides. For Hive compatibility, this name is entirely lowercase.

        table (string) --
        [REQUIRED]

        The name of the table to be deleted. For Hive compatibility, this name is entirely lowercase.

        """

        response = self.client['glue'].delete_table(
            DatabaseName=database,
            Name=table
        )
        return response
