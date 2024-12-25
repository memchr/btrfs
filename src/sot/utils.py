import os
from pathlib import Path
import click


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


EDITOR = os.environ.get("EDITOR", "vim")

def edit_annotation(annotation: str) -> str:
    edited = click.edit(annotation, editor=EDITOR)
    if edited is not None:
        return edited.strip()
    return annotation
