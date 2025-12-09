import os
import json
import yaml
import boto3

from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource
from msk_iam_auth import MSKAuthTokenProvider


S3_BUCKET = os.environ["TOPIC_CONFIG_BUCKET"]
S3_KEY = os.environ["TOPIC_CONFIG_KEY"]
BOOTSTRAP_SERVERS_ENV = os.environ.get("BOOTSTRAP_SERVERS")

s3 = boto3.client("s3")
auth_provider = MSKAuthTokenProvider()


def sasl_iam_auth_cb():
    token = auth_provider.get_auth_token()
    return token.token, token.expiration


def build_client(bootstrap_servers):
    return AdminClient({
        "bootstrap.servers": bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "OAUTHBEARER",
        "oauth_cb": sasl_iam_auth_cb,
    })


def load_config():
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    return yaml.safe_load(obj["Body"].read())


def handler(event, context):
    config = load_config()

    bs = BOOTSTRAP_SERVERS_ENV or config["bootstrap_servers"]
    admin = build_client(bs)

    existing = admin.list_topics(timeout=10).topics.keys()

    for t in config["topics"]:
        name = t["name"]
        partitions = t["partitions"]
        rf = t["replication_factor"]
        cfg = t.get("config", {})

        if name not in existing:
            admin.create_topics([NewTopic(name, partitions, rf, config=cfg)])

        # Update configs
        cr = ConfigResource("topic", name, set_config=cfg)
        admin.alter_configs([cr])

    return {"status": "success"}
