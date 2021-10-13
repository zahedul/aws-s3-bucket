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
        inquirer.Text("bucket", message="Bucket Name", default="m2a-dazndta-compliance"),
    ]

    return questions


def update_s3_object_tagging(session, bucket_name, content_key):
    client = session.client("s3")
    response = client.get_object_tagging(Bucket=bucket_name, Key=content_key)
    tags = response.get("TagSet")
    if not tags:
        logger.info(f"adding tag for {content_key}")
        outlet = content_key.split(".")[-3]
        logger.info(f"adding {outlet} tag to {content_key}")
        response = client.put_object_tagging(
            Bucket=bucket_name,
            Key=content_key,
            Tagging={
                'TagSet': [
                    {
                        'Key': 'outlet',
                        'Value': outlet
                    },
                ]
            }
        )
        logger.info(response)
    else:
        logger.info(tags)


def read_bucket(session, bucket_name):
    s3 = session.resource("s3")
    bucket = s3.Bucket(bucket_name)
    i = 1
    for content in bucket.objects.all():
        logger.info(f" {i} - {content.key}")
        yield content.key
        i += 1


def create_session(profile):
    return boto3.session.Session(profile_name=profile)


def main():
    answers = inquirer.prompt(basic_questions())
    profile = answers.get("profile")
    bucket = answers.get("bucket")

    session = create_session(profile)
    for i in read_bucket(session, bucket):
        update_s3_object_tagging(session, bucket, i)


if __name__ == "__main__":
    main()
