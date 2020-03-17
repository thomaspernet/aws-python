import pandas as pd
import boto3, logging, io, os
#from sagemaker import get_execution_role
from botocore.exceptions import ClientError

class connect_S3():
    def __init__(self,client, bucket):
        self.client =client
        self.bucket = bucket
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

    def upload_file(self, key):
        """Upload a file to an S3 bucket
        filename is deduce from key

        If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        client_boto = self.client['resource']
        filename = os.path.split(key)[1]

    # Upload the file
    try:
        client_boto.Bucket(self.bucket).upload_file(key, filename)
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

    def copy_object_s3(self,filename, destination):
        """
        filename -> include subfolder+filaneme
        ex: 'data/MR01_R_20200103.gz'
        destination: -> include subfolder+filaneme
        ex: 'data_sql/MR01_R_20200103.gz'
        """

        copy_source = {
        'Bucket': self.bucket,
        'Key': filename
 }
        try:
            self.client['resource'].meta.client.copy(
            copy_source,
             self.bucket,
              destination)
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
