import os
import zipfile
import boto
import logging

from datetime import datetime
from boto.s3.key import Key
from config import get_config


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def upload_data_dump(root_dir):
    logger.debug('Start archive data and upload dump to S3')
    output_file_path = archive_data_dump(root_dir=root_dir)
    upload_to_s3(get_config().AWS_S3_BUCKET_NAME, output_file_path)


def archive_data_dump(root_dir):
    logger.debug(f'Archiving {root_dir}')
    time_fmt = datetime.now().strftime(get_config().dump_file_format)
    filename = f'edp-data-{time_fmt}.zip'
    output_file_path = get_config().dump_output_path / filename
    with zipfile.ZipFile(str(output_file_path), 'w', zipfile.ZIP_DEFLATED) as zip_file:
        # TODO: output_file_path.relative_to()

        for root, dirs, files in os.walk(root_dir): # TODO: iterdir
            for file in files:
                file_path = os.path.join(root, file)
                rel_path = file_path[len(str(root_dir))+len(os.sep):]
                zip_file.write(file_path, rel_path)
        zip_file.close()
    logger.debug(f'Archive output file: {output_file_path}')
    return output_file_path


def upload_to_s3(bucket_name, file_path):
    logger.debug(f'Uploading archive file {file_path} to bucket {bucket_name} on s3')
    s3_connection = boto.connect_s3()
    bucket = s3_connection.get_bucket(bucket_name, validate=False)
    key = boto.s3.key.Key(bucket, file_path.name)
    key.set_contents_from_filename(file_path)
    logger.debug(f'Done successfully. {file_path} uploaded to s3')


if __name__ == '__main__':
    upload_data_dump(root_dir=get_config().input_path)
