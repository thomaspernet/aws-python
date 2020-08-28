import pandas as pd
import boto3, os
#from sagemaker import get_execution_role
#import dask.dataframe as dd

class connect_athena():
    def __init__(self, client = None, bucket=None, credentials = None):
        """
        crediitnals is a list
        """
        self.client =client
        self.bucket = bucket

    def run_query(self, query, database, s3_output, filename = None,
    destination_key = None):
        """
        s3_output -> 'output_sql'
        If filename != None, then return pandas dataframe
        no extension in filename
        Add kwarg ..
        """


        full_s3_output = 's3://{0}/{1}/'.format(self.bucket, s3_output)

        client = self.client['athena']
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
                },
            ResultConfiguration={
            'OutputLocation': full_s3_output,
            }
        )
        print('Execution ID: ' + response['QueryExecutionId'])
        if filename != None:
            results = False

            while results != True:
                if destination_key != None:
                    source_key = os.path.join(
                    destination_key,
                     '{}.csv'.format(output['QueryExecutionId'])
                    )

                    destination_key_filename = os.path.join(
                    destination_key,
                    '{}.csv'.format(filename)
                    )

                    results = s3.copy_object_s3(
                                                    source_key = source_key,
                                                    destination_key = destination_key_filename,
                                                    remove = True
                                                )
                    #key_file = 'XX/{}'.format(filename)

            table = (s3.read_df_from_s3(
                        key = destination_key_filename, sep = ',')
                        )
            return table
        else:
            return response
