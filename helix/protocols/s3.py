"""
S3 protocol client for Cohesity S3-compatible object storage.

Wraps boto3 with Cohesity-specific endpoint configuration.
Key features:
- Multipart upload with parallel parts and automatic abort on failure
- Content integrity via sha256 checksums
- Lifecycle policy helpers for expiration testing
"""

from __future__ import annotations

import hashlib
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, BinaryIO

from helix.protocols.base import ProtocolClient

logger = logging.getLogger(__name__)

try:
    import boto3
    import botocore.exceptions
    _BOTO3_AVAILABLE = True
except ImportError:
    _BOTO3_AVAILABLE = False
    logger.warning("boto3 not installed — S3Client will not function")


# Default multipart threshold: files larger than this use multipart upload
MULTIPART_THRESHOLD_BYTES = 100 * 1024 * 1024   # 100MB
MULTIPART_CHUNK_SIZE_BYTES = 100 * 1024 * 1024  # 100MB per part
MULTIPART_MAX_WORKERS = 4


class S3Client(ProtocolClient):
    """
    Boto3-backed S3 client targeting Cohesity's S3-compatible endpoint.

    Authentication: access key + secret key (not Helios API key).
    Endpoint: Cohesity cluster's S3 port (default HTTP port 80 or HTTPS 443).
    """

    def __init__(self, config: Any) -> None:
        if hasattr(config, "s3_endpoint"):
            self._endpoint = config.s3_endpoint
            self._access_key = config.s3_access_key
            self._secret_key = config.s3_secret_key
            self._region = getattr(config, "s3_region", "us-east-1")
        else:
            self._endpoint = config.get("s3_endpoint", "")
            self._access_key = config.get("s3_access_key", "")
            self._secret_key = config.get("s3_secret_key", "")
            self._region = config.get("s3_region", "us-east-1")

        self._client = None

    @property
    def protocol_name(self) -> str:
        return "s3"

    def connect(self) -> None:
        if not _BOTO3_AVAILABLE:
            raise RuntimeError("boto3 not installed — run: pip install boto3")
        self._client = boto3.client(
            "s3",
            endpoint_url=self._endpoint,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            region_name=self._region,
            config=boto3.session.Config(signature_version="s3v4"),
        )
        logger.info("S3 client connected to %s", self._endpoint)

    def disconnect(self) -> None:
        self._client = None

    def write_file(self, remote_path: str, data: bytes) -> None:
        """Upload bytes as S3 object. remote_path = 'bucket/key' format."""
        bucket, key = self._parse_path(remote_path)
        self._client.put_object(Bucket=bucket, Key=key, Body=data)

    def read_file(self, remote_path: str) -> bytes:
        bucket, key = self._parse_path(remote_path)
        response = self._client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()

    def list_directory(self, remote_path: str) -> list[str]:
        """List objects with prefix. remote_path = 'bucket' or 'bucket/prefix'."""
        parts = remote_path.strip("/").split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        paginator = self._client.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    def delete_file(self, remote_path: str) -> None:
        bucket, key = self._parse_path(remote_path)
        self._client.delete_object(Bucket=bucket, Key=key)

    def create_bucket(self, bucket_name: str) -> None:
        try:
            self._client.create_bucket(Bucket=bucket_name)
        except self._client.exceptions.BucketAlreadyOwnedByYou:
            pass

    def delete_bucket(self, bucket_name: str, force: bool = False) -> None:
        """Delete bucket, optionally emptying it first."""
        if force:
            for key in self.list_directory(bucket_name):
                self._client.delete_object(Bucket=bucket_name, Key=key)
        self._client.delete_bucket(Bucket=bucket_name)

    def upload_large_object(
        self,
        bucket: str,
        key: str,
        data: bytes | BinaryIO,
        chunk_size: int = MULTIPART_CHUNK_SIZE_BYTES,
    ) -> str:
        """
        Multipart upload with parallel parts.
        Returns sha256 checksum of the uploaded data for integrity verification.
        Always calls abort_multipart_upload on failure — no orphaned parts.
        """
        if isinstance(data, bytes):
            chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]
        else:
            chunks = []
            while True:
                chunk = data.read(chunk_size)
                if not chunk:
                    break
                chunks.append(chunk)

        # Compute checksum before upload
        h = hashlib.sha256()
        for chunk in chunks:
            h.update(chunk)
        checksum = h.hexdigest()

        mpu = self._client.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = mpu["UploadId"]
        parts = []
        try:
            def upload_part(args):
                part_number, chunk = args
                resp = self._client.upload_part(
                    Bucket=bucket, Key=key, UploadId=upload_id,
                    PartNumber=part_number, Body=chunk,
                )
                return {"PartNumber": part_number, "ETag": resp["ETag"]}

            with ThreadPoolExecutor(max_workers=MULTIPART_MAX_WORKERS) as pool:
                parts = list(pool.map(upload_part, enumerate(chunks, start=1)))

            self._client.complete_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload_id,
                MultipartUpload={"Parts": sorted(parts, key=lambda p: p["PartNumber"])},
            )
            logger.info("Multipart upload complete: %s/%s (%d parts)", bucket, key, len(parts))
        except Exception:
            self._client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            raise

        return checksum

    def verify_integrity(self, bucket: str, key: str, expected_checksum: str) -> bool:
        """Download object and verify sha256 checksum."""
        data = self.read_file(f"{bucket}/{key}")
        actual = hashlib.sha256(data).hexdigest()
        return actual == expected_checksum

    def set_bucket_lifecycle(self, bucket: str, rules: list[dict]) -> None:
        """Set lifecycle rules for expiration testing."""
        self._client.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={"Rules": rules},
        )

    @staticmethod
    def _parse_path(remote_path: str) -> tuple[str, str]:
        """Split 'bucket/key' → ('bucket', 'key')."""
        parts = remote_path.strip("/").split("/", 1)
        if len(parts) < 2:
            raise ValueError(f"S3 path must be 'bucket/key', got: '{remote_path}'")
        return parts[0], parts[1]
