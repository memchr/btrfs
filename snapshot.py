#!/usr/bin/python
from datetime import date, datetime
import hashlib
import os
from pathlib import Path
from typing import Any, override
import btrfsutil
from btrfsutil import BtrfsUtilError
import click

HOME = Path.home()
ROOT = HOME
SNAPSHOT_STORE = ROOT / ".snapshots"

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


class Volume:
    def __init__(self, path: Path = None, name=None, exists=False) -> None:
        """
        Relative volume path is interepted as relative path to HOME if it cannot be
        found in current directory
        """
        if name is not None:
            path = unescape(name)
        path = ensure_path(path)
        if not path.is_absolute() and not path.exists():
            path = ROOT / path
        path = path.resolve()
        if exists:
            if not btrfsutil.is_subvolume(str(path)):
                raise NotASubvolume(path)
            if not path.exists():
                raise SubvolumeNotFound(path)
        self.path = path
        self.relative_path = path.relative_to(ROOT)
        self.name = escape(self.relative_path)

    @property
    def snapshots_store(self):
        return SNAPSHOT_STORE / self.name

    @property
    def snapshots(self) -> list["Snapshot"]:
        path = self.snapshots_store
        if not path.exists():
            return []
        return [Snapshot(self, s.name) for s in path.iterdir() if s.is_dir()]


class Snapshot:
    def __init__(self, volume: Volume, name: str) -> None:
        if name is None:
            while (volume.snapshots_store / (name := self.generate_name())).exists():
                pass

        self.name = name
        self.volume = volume
        self.path = self.volume.snapshots_store / self.name

    def exists(self) -> bool:
        pass

    def create(self) -> None:
        if self.path.exists():
            raise SnapshotExists(self.name)
        self.path.parent.mkdir(exist_ok=True, parents=True)
        try:
            btrfsutil.create_snapshot(
                str(self.volume.path), str(self.path), read_only=True
            )
        except BtrfsUtilError as e:
            raise click.ClickException(e)

    def delete(self) -> None:
        path = str(self.path)
        try:
            btrfsutil.set_subvolume_read_only(path, read_only=False)
            btrfsutil.delete_subvolume(path)
        except BtrfsUtilError as e:
            raise click.ClickException(e)
        pass

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
    if root is not None:
        global ROOT
        global SNAPSHOT_STORE
        ROOT = root.resolve()
        SNAPSHOT_STORE = ROOT / ".snapshots"


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


@cli.command()
@args.volume(required=False, exists=False)
def list(volume: Volume):
    """List all snapshots."""
    if volume is None:
        click.echo("Listing all snapshots...")
        volume_snapshots = {
            (v := Volume(name=d.name)).relative_path: (s.name for s in v.snapshots)
            for d in SNAPSHOT_STORE.iterdir()
            if d.is_dir()
        }
    else:
        volume_snapshots = {volume.relative_path: (s.name for s in volume.snapshots)}
    for volume, snapshots in volume_snapshots.items():
        click.secho(volume, fg="green", bold=True)
        for snapshot in snapshots:
            click.secho(f"  {snapshot}", fg="blue")


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
    snapshots: str,
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
            b = before.strftime(DATETIME_FORMAT)
            snapshots = [name for name in volume.snapshots if name < b]
    else:
        snapshots = [Snapshot(volume, name) for name in snapshots]
    if len(snapshots) == 0:
        raise click.UsageError(
            "No snapshots available for deletion. Specify the snapshots to delete using filter options or by their names."
        )
    if dry_run:
        click.echo("Dry run, no snapshots will be deleted...")
    else:
        click.echo("Deleting snapshots...")

    name_s = click.style(volume.relative_path, fg="green", bold=True)
    for s in snapshots:
        if not dry_run:
            pass
            # delete_subvolume(snapshots_path / s)
        click.echo(f"Deleted snapshot: '{name_s}/{click.style(s, fg="blue")}'")
    if all:
        if not dry_run:
            pass
            # snapshots_path.rmdir()
        click.echo(f"Removed snapshots dir for subvolume {name_s}")


if __name__ == "__main__":
    try:
        cli()
    except FileNotFoundError as e:
        print(e)
