import hashlib

from utils.archive.checksums import sha256_file, verify_sha256


def test_streaming_sha256_and_verification(tmp_path):
    payload = (b"sporely-checksum" * 10000) + b"end"
    path = tmp_path / "large.bin"
    path.write_bytes(payload)
    expected = hashlib.sha256(payload).hexdigest()
    assert sha256_file(path, chunk_size=17) == expected
    assert verify_sha256(path, expected, chunk_size=19)
    assert not verify_sha256(path, "A" * 64)
    assert not verify_sha256(path, "short")
