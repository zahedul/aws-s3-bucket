import logging
import os
import random
import string
from urllib import parse

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


def get_bucket_lifecycle(session, bucket):
    s3 = session.resource('s3')
    bucket_lifecycle = s3.BucketLifecycle(bucket)
    logger.info(bucket_lifecycle.load())


def create_session(profile):
    return boto3.session.Session(profile_name=profile)


def get_uploaded_file():
    files = os.listdir("files")
    selected_file = files[random.randint(0, len(files) - 1)]
    return f"files/{selected_file}"


def get_tag():
    tags = [
        {"outlet": "us", "validation_time": "1"},
        {"outlet": "uk", "validation_time": "2"},
        {"outlet": "bd", "validation_time": "3"}
    ]

    return tags[random.randint(0, len(tags) - 1)]


def main():
    answers = inquirer.prompt(basic_questions())
    profile = answers.get("profile")
    bucket = answers.get("bucket")

    session = create_session(profile)
    tag = get_tag()

    get_bucket_lifecycle(session, bucket)


if __name__ == "__main__":
    main()
