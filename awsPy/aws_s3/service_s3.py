import pandas as pd
import boto3, logging, io, os
#from sagemaker import get_execution_role
from botocore.exceptions import ClientError
import pytz
from datetime import datetime

class connect_S3():
    def __init__(self,client, bucket, verbose = True):
        self.client =client
        self.bucket = bucket
        self.verbose = verbose
#### S3
    def download_file(self, key):
        """
        key -> key from S3
        """
        #if subfolder is None:
        #paths3 = '{}/{}'.format(self.bucket, file_name)

        client_boto = self.client['resource']
        filename = os.path.split(key)[1]

    # Download the file
        try:
            client_boto.Bucket(self.bucket).download_file(key, filename)
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise

    def upload_file(self, file_to_upload, destination_in_s3):
        """Upload a file to an S3 bucket
        filename is deduce from key

        If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        client_boto = self.client['resource']
        filename = os.path.split(file_to_upload)[1]
        key = '{}/{}'.format(destination_in_s3, filename)

        # Upload the file
        try:
            client_boto.Bucket(self.bucket).upload_file(file_to_upload, key)
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise

    def create_folder(self, directory_name):
        """
        """
        try:
            self.client['s3'].put_object(Bucket=self.bucket,
              Key=(directory_name))
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def copy_object_s3(self,source_key, destination_key, other_bucket = None,
                      remove = False, verbose = False):
        """
        filename -> include subfolder+filaneme
        ex: 'data/MR01_R_20200103.gz'
        destination: -> include subfolder+filaneme
        ex: 'data_sql/MR01_R_20200103.gz'
        other_bucket: dictionary with {'origin_bucket : '', 'destination_bucket:''}
        """

        if other_bucket == None:
            bucket_source = self.bucket
            bucket_dest = self.bucket
        else:
            bucket_source = other_bucket['origin_bucket']
            bucket_dest = other_bucket['destination_bucket']

        copy_source = {
       'Bucket': bucket_source,
       'Key': source_key
}

        try:
            self.client['resource'].meta.client.copy(
            copy_source,
            bucket_dest,
            destination_key)

            if remove:
               self.client['resource'].Object(self.bucket,
                                         source_key).delete()
        except ClientError as e:
            if self.verbose:
                logging.error(e)
            return False
        return True

    def move_object_s3(self, source_key, destination_key, remove = True):
        """
        destination key should include name or new name
        """

        source = "{}/{}".format(self.bucket,
                                     source_key)

        try:
            self.client['resource'].Object(
                self.bucket,
                destination_key).copy_from(
                CopySource=source)

            if remove:
                self.client['resource'].Object(self.bucket,
                                          source_key).delete()
                print("File {} is deleted".format(source_key))
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def remove_file(self, key):
        """
        """
        try:
            self.client['resource'].Object(self.bucket,
                                          key).delete()
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def remove_all_bucket(self, path_remove):
        """
        """
        try:
            my_bucket = self.client['resource'].Bucket(
                self.bucket)
            for item in my_bucket.objects.filter(Prefix=path_remove):
                item.delete()
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def remove_all_folder_date_modified(self,
                                        path_remove,
                                        date_filter,
                                        timezone = "Europe/Paris"):
        """Remove all files in folder if above date modified
        date_filter format -> %Y/%m/%d
        """
        py_timezone = pytz.timezone(timezone)
        date_filter = py_timezone.localize(
            datetime.strptime("{} 00:00:00".format(date_filter),
                           "%Y/%m/%d %H:%M:%S")).astimezone(py_timezone)
        try:
            my_bucket = self.client['resource'].Bucket(
                self.bucket)
            for item in my_bucket.objects.filter(Prefix=path_remove):
                if item.last_modified.astimezone(py_timezone) > date_filter:
                    item.delete()
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def read_df_from_s3(self, key, sep = ',',encoding = None):
        """
        key is the key in S3
        No Dask supported yet
        """
        obj = self.client['resource'].Object(self.bucket, key)

        body = obj.get()['Body'].read()

        df_ = pd.read_csv(
            io.BytesIO(body),
            sep = sep,
            encoding=encoding,
            low_memory=False,
            error_bad_lines=False)

        return df_

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
                     '{}.csv'.format(response['QueryExecutionId'])
                    )

                    destination_key_filename = os.path.join(
                    destination_key,
                    '{}.csv'.format(filename)
                    )

                    results = self.copy_object_s3(
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
