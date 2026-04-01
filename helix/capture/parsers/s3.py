"""
S3/HTTP packet parser — extracts S3 API metadata from tshark JSON frames.

Fields validated:
  - HTTP methods and response codes (PUT 200, GET 200, DELETE 204, errors)
  - Multipart upload sequence completeness
  - Content-MD5 header presence (data integrity)
  - TLS cipher suite (for HTTPS S3 tests)

tshark fields used:
    http.request.method       (GET, PUT, DELETE, POST, HEAD)
    http.request.uri          (path — detect ?uploads, ?partNumber, ?uploadId)
    http.response.code        (200, 204, 403, 404, 500)
    http.content_type
    http.request.line         (raw headers including Content-MD5)
    tls.handshake.ciphersuite (TLS cipher negotiated)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class S3ParseResult:
    method_counts: dict[str, int] = field(default_factory=dict)     # method -> count
    response_codes: dict[str, int] = field(default_factory=dict)    # code -> count
    has_multipart_upload: bool = False
    multipart_init_count: int = 0          # POST ?uploads
    multipart_part_count: int = 0          # PUT ?partNumber=N
    multipart_complete_count: int = 0      # POST ?uploadId=X
    multipart_abort_count: int = 0         # DELETE ?uploadId=X
    content_md5_present: bool = False
    tls_cipher: str | None = None
    frame_count: int = 0
    error_count: int = 0                   # 4xx/5xx responses

    @property
    def multipart_sequence_complete(self) -> bool:
        """True if every initiated multipart upload was completed or aborted."""
        terminated = self.multipart_complete_count + self.multipart_abort_count
        return self.multipart_init_count == 0 or terminated >= self.multipart_init_count

    @property
    def success_rate(self) -> float:
        total = sum(self.response_codes.values())
        if total == 0:
            return 1.0
        return (total - self.error_count) / total


class S3Parser:
    """
    Parse tshark JSON frames for S3 HTTP/HTTPS protocol validation.

    Usage:
        frames = TsharkCapture(...).load_frames()
        result = S3Parser(frames).parse()
        assert result.multipart_sequence_complete, "Multipart upload not completed"
        assert result.content_md5_present, "Missing Content-MD5 integrity header"
    """

    def __init__(self, frames: list[dict[str, Any]]) -> None:
        self._frames = frames

    def parse(self) -> S3ParseResult:
        result = S3ParseResult()
        result.frame_count = len(self._frames)

        for frame in self._frames:
            layers = frame.get("_source", {}).get("layers", {})
            self._parse_frame(layers, result)

        logger.debug(
            "S3Parser: %d frames, methods=%s, multipart=%s, errors=%d",
            result.frame_count, result.method_counts,
            result.has_multipart_upload, result.error_count
        )
        return result

    def _parse_frame(self, layers: dict[str, Any], result: S3ParseResult) -> None:
        http = layers.get("http", {})
        tls = layers.get("tls", {})

        # TLS cipher
        cipher = tls.get("tls.handshake.ciphersuite")
        if cipher and not result.tls_cipher:
            result.tls_cipher = cipher

        if not http:
            return

        # HTTP method
        method = http.get("http.request.method")
        if method:
            result.method_counts[method] = result.method_counts.get(method, 0) + 1
            uri = http.get("http.request.uri", "")
            self._classify_s3_operation(method, uri, result)

        # HTTP response code
        code = http.get("http.response.code")
        if code:
            result.response_codes[code] = result.response_codes.get(code, 0) + 1
            if code.startswith(("4", "5")):
                result.error_count += 1
                logger.warning("S3Parser: HTTP error %s", code)

        # Content-MD5 header
        headers = http.get("http.request.line", [])
        if isinstance(headers, list):
            for header in headers:
                if "content-md5" in str(header).lower():
                    result.content_md5_present = True
        elif "Content-MD5" in str(headers):
            result.content_md5_present = True

    def _classify_s3_operation(self, method: str, uri: str, result: S3ParseResult) -> None:
        """Classify multipart upload operations from URI query params."""
        if method == "POST" and "uploads" in uri and "uploadId" not in uri:
            # POST /bucket/key?uploads — initiate multipart
            result.multipart_init_count += 1
            result.has_multipart_upload = True

        elif method == "PUT" and "partNumber" in uri:
            # PUT /bucket/key?partNumber=N&uploadId=X — upload part
            result.multipart_part_count += 1

        elif method == "POST" and "uploadId" in uri:
            # POST /bucket/key?uploadId=X — complete multipart
            result.multipart_complete_count += 1

        elif method == "DELETE" and "uploadId" in uri:
            # DELETE /bucket/key?uploadId=X — abort multipart
            result.multipart_abort_count += 1
