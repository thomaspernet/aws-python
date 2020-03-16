import pandas as pd
import boto3, logging, io
#from sagemaker import get_execution_role
from botocore.exceptions import ClientError

class connect_S3():
    def __init__(self,client, bucket):
        self.client =client
        self.bucket = bucket
#### S3
    def download_file(self, file_name):
        """
        TO IMPROVE!!
        """
        #if subfolder is None:
        paths3 = '{}/{}'.format(self.bucket, file_name)

        client_boto = self.client['s3']

    # Upload the file
        try:
            response = client_boto.download_file(
                self.bucket,paths3, file_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def upload_file(self, bucket, file_name, subfolder=None):
        """Upload a file to an S3 bucket
        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name.
        If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if subfolder is None:
            subfolder = file_name

        client_boto = self.client['s3']

    # Upload the file
        try:
            response = client_boto.upload_file(
                file_name, self.bucket, subfolder)
        except ClientError as e:
            logging.error(e)
            return False
        return True

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

    def read_df_from_s3(self, key):
        """
        key is the key in S3
        No Dask supported yet
        """
        obj = self.client['resource'].Object(self.bucket, key)

        body = obj.get()['Body'].read()

        df_ = pd.read_csv(
            io.BytesIO(body),
            sep = None,
            engine='python',
            error_bad_lines=False)

        return df_
