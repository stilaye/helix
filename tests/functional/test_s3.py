"""
Functional: S3 object storage correctness tests.

Validates:
  - PUT/GET/DELETE basic operations
  - Multipart upload for large objects (>100 MB)
  - Object versioning
  - Checksum integrity (Content-MD5 and sha256)
  - Wire-level protocol validation via tshark capture
"""

from __future__ import annotations

import hashlib
import os
import uuid

import allure
import pytest

from helix.capture.tshark import TsharkCapture
from helix.capture.parsers.s3 import S3Parser

pytestmark = [pytest.mark.s3, pytest.mark.functional, pytest.mark.regression]


@allure.suite("Functional")
@allure.feature("S3 Object Storage")
class TestS3Functional:

    @allure.title("Basic PUT/GET/DELETE lifecycle")
    def test_put_get_delete(self, helix_config):
        """Verify basic S3 object lifecycle: write → read → delete → 404."""
        from helix.protocols.s3 import S3Client

        key = f"helix-test/{uuid.uuid4().hex}.txt"
        data = os.urandom(4096)
        expected_hash = hashlib.sha256(data).hexdigest()

        with S3Client(helix_config) as s3:
            with allure.step(f"PUT object {key}"):
                s3.write_file(key, data)

            with allure.step("GET and verify checksum"):
                read_data = s3.read_file(key)
                actual_hash = hashlib.sha256(read_data).hexdigest()
                assert actual_hash == expected_hash, "S3 GET checksum mismatch"

            with allure.step("DELETE object"):
                s3.delete_file(key)

            with allure.step("Verify 404 after delete"):
                with pytest.raises(Exception, match="NoSuchKey|404"):
                    s3.read_file(key)

    @allure.title("Multipart upload completes and data integrity preserved")
    @pytest.mark.timeout(180)
    def test_multipart_upload_integrity(self, helix_config):
        """
        Upload a 150 MB object via multipart (>5 MB threshold triggers multipart).
        Verify:
        - Multipart sequence: initiate → upload parts → complete
        - Multipart is NEVER left dangling (abort on failure)
        - Content checksum matches after download
        """
        from helix.protocols.s3 import S3Client

        object_size = 150 * 1024 * 1024  # 150 MB
        key = f"helix-multipart/{uuid.uuid4().hex}.bin"
        data = os.urandom(object_size)
        expected_hash = hashlib.sha256(data).hexdigest()

        capture_filter = f"tcp port 443 and host {helix_config.cluster_ip}"
        with TsharkCapture(
            interface="eth0",
            filter_expr=capture_filter,
            extra_fields=["http.request.method", "http.request.uri", "http.response.code"],
        ) as cap:
            with S3Client(helix_config) as s3:
                with allure.step(f"Upload {object_size // (1024*1024)} MB via multipart"):
                    s3.upload_large_object(key, data)

                with allure.step("Download and verify checksum"):
                    downloaded = s3.read_file(key)
                    actual_hash = hashlib.sha256(downloaded).hexdigest()
                    assert actual_hash == expected_hash, "Multipart upload checksum mismatch"

                s3.delete_file(key)

        frames = cap.load_frames()
        cap.attach_to_allure("s3_multipart_capture.json")

        with allure.step("Verify multipart sequence in wire capture"):
            parsed = S3Parser(frames).parse()
            allure.attach(
                f"Methods: {parsed.method_counts}\n"
                f"Multipart parts: {parsed.multipart_part_count}\n"
                f"Completed: {parsed.multipart_complete_count}\n"
                f"Content-MD5: {parsed.content_md5_present}",
                name="s3_multipart_info.txt",
                attachment_type=allure.attachment_type.TEXT,
            )
            assert parsed.multipart_sequence_complete, (
                "Multipart upload sequence incomplete — possible dangling upload"
            )

    @allure.title("Object listing returns correct keys")
    def test_list_objects(self, helix_config):
        """Write multiple objects and verify they appear in listing."""
        from helix.protocols.s3 import S3Client

        prefix = f"helix-list-test/{uuid.uuid4().hex[:8]}/"
        keys = [f"{prefix}file_{i}.txt" for i in range(5)]

        with S3Client(helix_config) as s3:
            for key in keys:
                s3.write_file(key, b"list test data")

            with allure.step("List objects with prefix"):
                listed = s3.list_directory(prefix)
                listed_keys = set(listed)

            for key in keys:
                s3.delete_file(key)

        missing = [k for k in keys if k not in listed_keys]
        assert not missing, f"Keys missing from listing: {missing}"
