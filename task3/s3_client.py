import os
import json
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "reports")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

CDN_BASE_URL = os.getenv("CDN_BASE_URL", "http://localhost:8088/cdn")

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name=S3_REGION,
    config=Config(signature_version="s3v4"),
)

def ensure_bucket():
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
    except ClientError:
        s3.create_bucket(Bucket=S3_BUCKET)

def s3_key_for_report(user_id: str, from_date: str, to_date: str, watermark_iso: str) -> str:
    safe_wm = watermark_iso.replace(":", "-")
    return f"reports/{user_id}/{from_date}/{to_date}/v={safe_wm}/report.json"

def cdn_url_for_key(key: str) -> str:
    # nginx будет проксировать /cdn/<bucket>/<key>
    return f"{CDN_BASE_URL}/{S3_BUCKET}/{key}"

def s3_exists(key: str) -> bool:
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
            return False
        # для NoSuchKey тоже сюда
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise

def s3_put_json(key: str, payload: dict):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8",
        CacheControl="public, max-age=3600",
    )