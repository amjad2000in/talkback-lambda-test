import boto3


class StreamingMultipartUpload:
    """
    Adapted from https://gist.github.com/teasherm/bb73f21ed2f3b46bc1c2ca48ec2c1cf5

    mpu = StreamingMultipartUpload(
        bucket,
        key,
        http_session,
        s3_metadata,
        part_size_bytes,
        region_name,
        verbose)

    mpu.go()


    """
    # AWS throws EntityTooSmall error for parts smaller than 5 MB
    PART_MINIMUM = int(5e6)

    def __init__(self,
                 bucket,
                 key,
                 http_response,
                 metadata,
                 total_bytes,
                 part_size_bytes=128 * 1024 * 1024,
                 storage_class="STANDARD_IA",
                 region_name="us-east-2",
                 verbose=False):
        self.bucket = bucket
        self.key = key
        self.http_response = http_response
        self.metadata = metadata
        self.total_bytes = total_bytes
        self.part_size_bytes = part_size_bytes
        self.storage_class = storage_class
        self.region_name = region_name
        self.verbose = verbose

        assert part_size_bytes > self.PART_MINIMUM

        if not self.total_bytes % part_size_bytes == 0:
            self.number_of_parts = int(self.total_bytes / part_size_bytes) + 1
            last_part_size = self.total_bytes % part_size_bytes
            if last_part_size < self.PART_MINIMUM:
                # Make the part size slightly bigger.
                # Will still maintain constant ram size
                self.part_size_bytes += int(last_part_size / self.number_of_parts) + 1

        if self.verbose:
            self.log('Uploading to s3 bucket {} and key {}', self.bucket, self.key)
            self.log('suggested part size {} bytes', str(part_size_bytes))
            self.log('final part size {} bytes', str(self.part_size_bytes))
            self.log('total size {} bytes', str(self.total_bytes))
            self.log('number of parts: {}', str(self.number_of_parts))

        assert (self.total_bytes % self.part_size_bytes == 0
                or self.total_bytes % self.part_size_bytes > self.PART_MINIMUM)

        self.s3_client = boto3.client(service_name='s3', region_name=region_name)
        self.s3_resource = boto3.resource(service_name='s3', region_name=region_name)

    def abort_all(self):
        mpus = self.s3_client.list_multipart_uploads(Bucket=self.bucket)
        aborted = []

        if "Uploads" in mpus:
            if self.verbose:
                self.log("Aborting {} prior uploads {}", len(mpus["Uploads"]), mpus["Uploads"])
            for u in mpus["Uploads"]:
                upload_id = u["UploadId"]
                aborted.append(
                    self.s3_client.abort_multipart_upload(
                        Bucket=self.bucket, Key=self.key, UploadId=upload_id))

        return aborted

    def create(self):
        mpu = self.s3_client.create_multipart_upload(Bucket=self.bucket,
                                                     Key=self.key,
                                                     Metadata=self.metadata,
                                                     StorageClass=self.storage_class,
                                                     ServerSideEncryption='AES256')
        mpu_id = mpu["UploadId"]
        return mpu_id

    def upload(self, mpu_id):
        parts = []
        uploaded_bytes = 0

        if self.verbose:
            self.log('About to upload')

        part_number = 1
        for chunk in self.http_response.iter_content(chunk_size=self.part_size_bytes):
            if chunk:
                part = self.s3_client.upload_part(
                    Body=chunk, Bucket=self.bucket, Key=self.key, UploadId=mpu_id, PartNumber=part_number)
                parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                uploaded_bytes += len(chunk)
                if self.verbose:
                    self.log("Part {0}: {1} of {2} uploaded ({3:.3f}%)".format(
                        part_number,
                        uploaded_bytes, self.total_bytes,
                        self.as_percent(uploaded_bytes,
                                        self.total_bytes)))
                part_number += 1

        return parts

    def complete(self, mpu_id, parts):
        result = self.s3_client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=mpu_id,
            MultipartUpload={"Parts": parts})
        return result

    @staticmethod
    def as_percent(num, denominator):
        return float(num) / float(denominator) * 100.0

    def go(self):
        """

        Returns
        -------
        s3_object: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#object

        """
        # abort all multipart uploads for this bucket (optional, for starting over)
        self.abort_all()

        # create new multipart upload
        mpu_id = self.create()
        # upload parts
        parts = self.upload(mpu_id)
        # complete multipart upload
        result = self.complete(mpu_id, parts)

        if self.verbose:
            self.log(result)

        s3_object = self.s3_resource.Object(self.bucket, self.key)
        return s3_object

    @staticmethod
    def log(message="None", *args):
        """
        Convenience wrapper to make it easier to use .format()
        """
        if type(message) is str:
            print(message.format(*args))
        else:
            print(message)
