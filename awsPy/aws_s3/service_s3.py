import pandas as pd
import boto3, logging, io, os
#from sagemaker import get_execution_role
from botocore.exceptions import ClientError
import pytz
from datetime import datetime

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
                      remove = False):
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

    def key_exist(self, key):
        '''
        Check existence of a file in S3
        
        Args:
        - key: full path of file in S3
        ex: 'INPI/TC_1/00_RawData/public/IMR_Donnees_Saisies/tc/stock/2019/11/25/9401_S7_20191125.zip'

        Return True if exists, False if not.
        '''
        
        s3_service = self.client['resource']
        try:
            s3_service.Object(self.bucket, key).load()
        except ClientError as e:
            return int(e.response['Error']['Code']) != 404
        return True

    def list_files_s3(self,prefix_):
        '''
        List all keys in a directory in S3
        
        Args:
        - prefix_: Directory in S3 to lookup.
        ex:'INPI/TC_1/Flux/2018/01/ACTES/NEW'
        
        Return:
        A list of all keys found in this directory (files + folders)
        ex: 
        ['INPI/TC_1/Flux/2018/01/ACTES/NEW/',
         'INPI/TC_1/Flux/2018/01/ACTES/NEW/0101_163_20180103_084810_12_actes.csv',
         'INPI/TC_1/Flux/2018/01/ACTES/NEW/0101_164_20180104_054316_12_actes.csv']
        '''
        
        bucket_name = self.client['resource'].Bucket(self.bucket)
        
        ## List objects within a given prefix
        list_files=[]
        for obj in bucket_name.objects.filter(Prefix=prefix_):
            list_files.append(obj.key)
        
        return list_files
