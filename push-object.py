import logging
import os
import random
import string

import boto3
from botocore.exceptions import ClientError
import inquirer

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)s] : %(message)s')
logger = logging.getLogger(__name__)


def basic_questions():
    questions = [
        inquirer.Text("profile", message="AWS Profile", default="dazntest"),
        inquirer.Text("bucket", message="Bucket Name", default="zahed-test"),
    ]

    return questions


def get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


def upload_file(session, bucket, file_name, object_name, extra_args):
    """Upload a file to an S3 bucket

    :param session: AWS session
    :param bucket: Bucket to upload to
    :param file_name: File to upload
    :param object_name: S3 object name. If not specified then file_name is used
    :param extra_args: extra args
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3 = session.resource('s3')
    s3_bucket = s3.Bucket(bucket)
    try:
        response = s3_bucket.upload_file(file_name, object_name, ExtraArgs={"Metadata": extra_args})
        logger.info(response)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def create_session(profile):
    return boto3.session.Session(profile_name=profile)


def get_uploaded_file():
    files = os.listdir("files")
    selected_file = files[random.randint(0, len(files)-1)]
    return f"files/{selected_file}"


def get_tag():
    tags = [
        {"outlet": "us", "validation_time": 1},
        {"outlet": "uk", "validation_time": 2},
        {"outlet": "bd", "validation_time": 3}
    ]

    return tags[random.randint(0, len(tags))]


def main():
    answers = inquirer.prompt(basic_questions())
    profile = answers.get("profile")
    bucket = answers.get("bucket")

    session = create_session(profile)
    tag = get_tag()
    file_name = f"{get_random_string(8)}_{tag.get('outlet')}.mp4"
    content_to_upload = get_uploaded_file()

    upload_file(session, bucket, content_to_upload, file_name, tag)


if __name__ == "__main__":
    main()
