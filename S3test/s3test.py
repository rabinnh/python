#!/usr/bin/env python3

'''

This example would take zero length files in s3://mybucket/123456/ and move them to s3://mybucket/zeros/123456
Example:
    bucket = 'zerolentest2'
    prefix = '123456'
    s3bucket = S3ZeroLenCheck(bucket)
    if (s3bucket.exists):
        list = s3bucket.listZeroLengthObjects(prefix)
        print(list)
        s3bucket.moveZeroLengthObjects(prefix, 'zerolentest2', 'zeros', list)


'''

import boto3
import botocore


# Class to access and check for 0 length files in an S3 bucket
class S3ZeroLenCheck(object):
    def __init__(self, S3bucket):
        self.S3bucket = S3bucket
        self.exists = self.bucketExists()

    # Check bucket exists
    def bucketExists(self):
        s3 = boto3.client('s3')
        exists = True
        try:
            s3.head_bucket(Bucket=self.S3bucket)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                exists = False
        return(exists)


    # Get zero length objects
    def listZeroLengthObjects(self, key):
        s3Resource = boto3.resource('s3')
        bucket = s3Resource.Bucket(self.S3bucket)
        s3 = boto3.client('s3')
        zeros = []
        for obj in bucket.objects.filter(Prefix=key):
            length = s3.head_object(Bucket=self.S3bucket, Key=obj.key)['ContentLength']
            if length == 0:
                okey = obj.key
                okey = okey[len(key):]
                if okey[0] == '/':
                    okey = okey[1:]
                zeros.append(okey)
        return(zeros)

    # Move zero length objects
    def moveZeroLengthObjects(self, prefix, destBucket, destPrefix, zeros):
        s3 = boto3.resource('s3')
        # For the zero length objects, copy them to some place that won't interfere
        for key in zeros:
            copy_source = {
                'Bucket': self.S3bucket,
                'Key': prefix + '/' + key
            }
            # Copy
            try:
                s3.meta.client.copy(copy_source, destBucket, destPrefix + '/' + prefix + '/' + key)
                # Remove
                s3.Object(self.S3bucket, prefix + '/' + key).delete()
            except:
                return(False, key)



