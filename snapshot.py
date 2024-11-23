#!/usr/bin/python
from __future__ import annotations
from datetime import date, datetime
import hashlib
import os
from pathlib import Path
import shutil
import time
from typing import Any, override
import btrfsutil
from btrfsutil import BtrfsUtilError
import click
import json


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"


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


class NotASubvolume(click.BadParameter):
    def __init__(self, path: str) -> None:
        super().__init__(f"'{path}' is not a btrfs subvolume.", param_hint="VOLUME")


class SubvolumeNotFound(click.BadParameter):
    def __init__(self, path: str) -> None:
        super().__init__(f"Subvolume '{path}' not found.", param_hint="VOLUME")


class SnapshotExists(click.BadParameter):
    def __init__(self, name: str) -> None:
        super().__init__(f"Snapshot '{name}' exists.", param_hint="VOLUME")


class NoSnapshotsError(click.BadParameter):
    def __init__(self, name: str) -> None:
        super().__init__(f"'{name}' does not have snapshots.", param_hint="VOLUME")


class SnapshotStorage:
    MetadataType = dict[str, dict[str, float]]

    def __init__(self, root: Path) -> None:
        self.root = ensure_path(root).resolve()
        self.path = root / ".snapshots"
        self._json = self.path / "index.json"
        # silly, but not as much as _ = self.metadata
        self._metadata_cached = self._metadata

    def __div__(self, volume) -> Path:
        return self.path / volume

    @property
    def _metadata(self) -> MetadataType:
        """ """
        if not self._json.exists():
            self._metadata = {}
        with self._json.open("r") as f:
            md = json.load(f)
        self._metadata_cached = md
        return md

    @_metadata.setter
    def _metadata(self, md: MetadataType):
        with self._json.open("w") as f:
            json.dump(
                {
                    k: dict(sorted(v.items(), key=lambda x: -x[1]))
                    for k, v in md.items()
                },
                f,
            )

    def unregister(self, obj: "Snapshot" | "Volume"):
        if isinstance(obj, Snapshot):
            md = self._metadata
            del md[obj.volume.name][obj.name]
            self._metadata = md
            pass
        elif isinstance(obj, Volume):
            raise NotImplementedError

    def register(self, obj: "Snapshot" | "Volume"):
        if isinstance(obj, Snapshot):
            md = self._metadata
            volume = obj.volume

            if volume.name in md:
                md[volume.name][obj.name] = time.time()
            else:
                md[volume.name] = {obj.name: time.time()}
            self._metadata = md
        elif isinstance(obj, Volume):
            raise NotImplementedError

    def query(self, obj: "Snapshot" | "Volume") -> "Snapshot" | "Volume":
        if isinstance(obj, Snapshot):
            raise NotImplementedError
        elif isinstance(obj, Volume):
            return [
                Snapshot(obj, name, time)
                for name, time in self._metadata_cached[obj.name].items()
            ]

    def update(self, obj: "Snapshot" | "Volume") -> "Snapshot" | "Volume":
        if isinstance(obj, Snapshot):
            raise NotImplementedError
        elif isinstance(obj, Volume):
            raise NotImplementedError

    def iter(self):
        for d in self.path.iterdir():
            if d.is_dir():
                yield d


class Volume:
    storage: SnapshotStorage

    def __init__(self, path: Path = None, name=None, exists=False) -> None:
        """
        Relative volume path is interepted as relative path to SubvolumeStorage
        if it cannot be found in current directory
        """
        path = ensure_path(path if name is None else unescape(name))
        self.path = path.resolve() if path.exists() else self.storage.root / path
        self.relative_path = self.path.relative_to(self.storage.root)
        self.name = escape(self.relative_path)
        self.snapshots_path = self.storage.path / self.name

        if exists:
            self.assert_is_volume()

    def assert_is_volume(self):
        path = self.path
        if not btrfsutil.is_subvolume(str(path)):
            raise NotASubvolume(path)
        if not path.exists():
            raise SubvolumeNotFound(path)

    @property
    def snapshots(self) -> list["Snapshot"]:
        path = self.snapshots_path
        if not path.exists():
            return []
        return self.storage.query(self)


class Snapshot:
    def __init__(self, volume: Volume, name: str, time: float = 0) -> None:
        if name is None:
            while (volume.snapshots_path / (name := self.generate_name())).exists():
                pass

        self.name = name
        self.volume = volume
        self.path = self.volume.snapshots_path / self.name
        self.time = time

    def create(self) -> None:
        if self.path.exists():
            raise SnapshotExists(self.name)
        self.path.parent.mkdir(exist_ok=True, parents=True)
        btrfsutil.create_snapshot(str(self.volume.path), str(self.path), read_only=True)
        self.volume.storage.register(self)

    def delete(self) -> None:
        path = str(self.path)
        btrfsutil.set_subvolume_read_only(path, read_only=False)
        btrfsutil.delete_subvolume(path)
        self.volume.storage.unregister(self)
        pass

    @property
    def strtime(self):
        return datetime.fromtimestamp(self.time).strftime(DATETIME_FORMAT)

    @staticmethod
    def generate_name():
        return hashlib.sha256(os.urandom(16)).hexdigest()[:8]


class VolumeParamType(click.ParamType):
    name = "volume"

    def __init__(self, exists=False) -> None:
        self.exists = exists
        super().__init__()

    def convert(
        self, value: Any, param: click.Parameter | None, ctx: click.Context | None
    ) -> Any:
        if isinstance(value, Volume):
            return value
        try:
            return Volume(value, exists=self.exists)
        except (NotASubvolume, SubvolumeNotFound) as e:
            self.fail(e, param, ctx)


class args:
    @staticmethod
    def volume(required=True, exists=True):
        return click.argument(
            "volume",
            type=VolumeParamType(exists=exists),
            required=required,
        )


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option(
    "-r",
    "--root",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="btrfs root",
)
def cli(root: Path):
    """BTRFS snapshots management."""
    Volume.storage = SnapshotStorage(root if root is not None else Path.home())


@cli.command()
@args.volume(exists=True)
@click.argument("name", type=click.STRING, required=False)
def create(volume: Volume, name):
    """Create new snapshot."""
    snapshot = Snapshot(volume, name)
    snapshot.create()
    click.echo(
        f"Snapshot '{click.style(volume.name, fg="green", bold=True)}/{click.style(snapshot.name, fg="blue")}' created"
    )


@cli.command(name="list")
@args.volume(required=False, exists=False)
def list_(volume: Volume):
    """List all snapshots."""
    volumes_snapshots: dict[Volume, list[Snapshot]]

    if volume is None:
        click.echo("Listing all snapshots...")
        volumes_snapshots = {
            (v := Volume(name=d.name)).relative_path: v.snapshots
            for d in Volume.storage.iter()
        }
    else:
        volumes_snapshots = {volume.relative_path: volume.snapshots}

    leftpad = min(shutil.get_terminal_size().columns - 16, 60)
    for volume, snapshots in volumes_snapshots.items():
        click.secho(volume, fg="green", bold=True)
        for snapshot in snapshots:
            click.echo(
                f"  {click.style(snapshot.name, fg="yellow"):<{leftpad}} {snapshot.strtime}"
            )


class _DateTime(click.DateTime):
    @override
    def convert(self, value, *args, **kwargs) -> Any:
        if value == "today":
            return date.today()
        return super().convert(value, *args, **kwargs)


@cli.command()
@args.volume(exists=False)
@click.argument("snapshots", type=click.STRING, required=False, nargs=-1)
@click.option(
    "-n",
    "--dry-run",
    is_flag=True,
    help="Print what would be done without deleting snapshots",
)
@click.option("-k", "--keep", type=int, help="Number of lastest snapshots to keep")
@click.option("-b", "--before", type=_DateTime(), help="Delete snapshots before date")
@click.option("-a", "--all", is_flag=True, help="Delete all snapshots")
def delete(
    volume: Volume,
    before: datetime,
    snapshots: list[Snapshot],
    dry_run: bool,
    keep: int,
    all: bool,
):
    """Delete snapshots."""
    if len(snapshots) == 0:
        if all:
            snapshots = volume.snapshots
        elif keep is not None:
            snapshots = volume.snapshots[:-keep]
        elif before is not None:
            raise NotImplementedError
            # b = before.strftime(DATETIME_FORMAT)
            # snapshots = [name for name in volume.snapshots if name < b]
        if len(snapshots) == 0:
            raise click.UsageError("No snapshots available for deletion.")
    else:
        snapshots = [Snapshot(volume, name) for name in snapshots]
    if dry_run:
        click.echo("Dry run, no snapshots will be deleted...")
    else:
        click.echo("Deleting snapshots...")

    name_s = click.style(volume.relative_path, fg="green", bold=True)
    for s in snapshots:
        if not dry_run:
            try:
                s.delete()
            except (Warning, BtrfsUtilError) as e:
                click.echo(e, err=True)

            # delete_subvolume(snapshots_path / s)
        click.echo(f"Deleted snapshot: '{name_s}/{click.style(s.name, fg="blue")}'")
    if all:
        if not dry_run:
            volume.snapshots_path.rmdir()
        click.echo(f"Removed snapshots dir for subvolume {name_s}")


if __name__ == "__main__":
    try:
        cli()
    except FileNotFoundError as e:
        print(e)
