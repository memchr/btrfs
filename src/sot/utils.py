import os
from pathlib import Path


def escape(path: str) -> str:
    return (
        str(path).strip("/").replace(r"%", r"%%").replace("@", r"%t").replace("/", "@")
    )


def unescape(path: str) -> str:
    return str(path).replace("@", "/").replace("%t", "@").replace(r"%%", "%")


def ensure_path(path: os.PathLike):
    if isinstance(path, Path):
        return path
    return Path(path)
