from pathlib import Path
from bisect import bisect_left

import boto3
import json


class S3Sync:
    """
    Class that holds the operations needed for synchronize local dirs to a given bucket.
    """

    def __init__(self):
        self._s3 = boto3.client('s3')

    def sync(self, source_bucket: str, source_prefix: str,  dest_bucket: str, dest_prefix: str) -> [str]:
        """
        Sync source to dest, this means that all elements existing in
        source that not exists in dest will be copied to dest.

        No element will be deleted.

        :param source: Source folder.
        :param dest: Destination folder.

        :return: None
        """

        src_objects = self.list_source_objects(src_bucket=source_bucket, src_path=source_prefix)
        trg_objects = self.list_target_objects(trg_bucket=dest_bucket, trg_path=dest_prefix)
        # Getting the keys and ordering to perform binary search
        # each time we want to check if any paths is already there.
        try:
            trg_obj_keys = [ob["Key"] for ob in trg_objects].sort()
        except Exception as e:
            trg_obj_keys = []
        s3 = boto3.resource("s3")  
       
       
        bucket = s3.Bucket(dest_bucket)
        paths = []
        for obj in src_objects:
            copy_source = {'Bucket': source_bucket,'Key': obj["Key"]}
            if trg_obj_keys is not None:
                if not obj["Key"].endswith("/") and  not (obj["Key"] in trg_obj_keys):
                    bucket.copy(copy_source, obj["Key"].replace(source_prefix,dest_prefix), ExtraArgs={"ServerSideEncryption":"aws:kms","SSEKMSKeyId":"alias/aws/s3"})
            elif trg_obj_keys is None and not obj["Key"].endswith("/") :
                print(obj["Key"].replace(source_prefix,dest_prefix))
                print(copy_source)
                bucket.copy(copy_source, obj["Key"].replace(source_prefix,dest_prefix), ExtraArgs={"ServerSideEncryption":"aws:kms","SSEKMSKeyId":"alias/aws/s3"})
           
               
        return {
            "status":"200: Successfully synched"
        }
       
    def list_target_objects(self, trg_bucket: str, trg_path: str) -> [dict]:
        """
        List all objects for the given bucket.

        :param bucket: Bucket name.
        :return: A [dict] containing the elements in the bucket.

        Example of a single object.

        {
            'Key': 'example/example.txt',
            'LastModified': datetime.datetime(2019, 7, 4, 13, 50, 34, 893000, tzinfo=tzutc()),
            'ETag': '"b11564415be7f58435013b414a59ae5c"',
            'Size': 115280,
            'StorageClass': 'STANDARD',
            'Owner': {
                'DisplayName': 'webfile',
                'ID': '75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a'
            }
        }

        """
        try:
            if trg_path is None:
                target_bucket = self._s3.list_objects_v2(Bucket=trg_bucket)['Contents']
            else:
                target_bucket = self._s3.list_objects_v2(Bucket=trg_bucket, Prefix=trg_path)['Contents']
        except Exception as e:
            target_bucket = []
        paths = []
        # if src_path is None:
        #     all_relevant_objects = source_bucket.objects.all()
        # else:
        #     all_relevant_objects = source_bucket.objects.filter(Prefix=src_path)
           
        # for object in all_relevant_objects:
        #     if object.key.endswith("/"):
        #         pass
        #     else:
        #         paths.append(object.key)
        return target_bucket


    def list_source_objects(self, src_bucket: str, src_path: str) -> [dict]:
        """
        :param src_bucket:  Root bucket for resources you want to list.
        :param src_path: path within the bucket
        :return: A [str] containing relative names of the files.

        Example:

            /tmp
                - example
                    - file_1.txt
                    - some_folder
                        - file_2.txt

            >>> sync.list_source_objects("/tmp/example")
            ['file_1.txt', 'some_folder/file_2.txt']

        """
        try:
            if src_path is None:
                source_bucket = self._s3.list_objects_v2(Bucket=src_bucket)['Contents']
            else:
                source_bucket = self._s3.list_objects_v2(Bucket=src_bucket, Prefix=src_path)['Contents']
        except Exception as e:
            source_bucket = []
        paths = []
        # if src_path is None:
        #     all_relevant_objects = source_bucket.objects.all()
        # else:
        #     all_relevant_objects = source_bucket.objects.filter(Prefix=src_path)
           
        # for object in all_relevant_objects:
        #     if object.key.endswith("/"):
        #         pass
        #     else:
        #         paths.append(object.key)
        return source_bucket


def lambda_handler(event, context):
    sync = S3Sync()
    return sync.sync(source_bucket="novartisrssr27devief1sttm001", source_prefix="sttm-dynamodb-backup", dest_bucket="novartisrssr27devief1sttm001", dest_prefix="sttm-dynamodb-backup1")
