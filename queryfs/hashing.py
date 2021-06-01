from hashlib import sha256
from pathlib import Path
from typing import Union


def hash_from_file(path: Union[str, Path]) -> str:
    sha256_hash = sha256()

    with open(path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)

    return sha256_hash.hexdigest()


def hash_from_bytes(buffer: bytes) -> str:
    sha256_hash = sha256()
    sha256_hash.update(buffer)

    return sha256_hash.hexdigest()
