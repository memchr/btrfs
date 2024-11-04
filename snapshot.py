#!/usr/bin/python
from datetime import date, datetime
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


def arg_volume(required=True, exists=True):
    return click.argument(
        "volume",
        type=click.Path(exists=exists, file_okay=False, path_type=Path),
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
@arg_volume()
def create(volume: Path):
    """Create new snapshot."""
    timestamp = datetime.now().strftime(DATETIME_FORMAT)
    volume, name = resolve_volume(volume, exists=True)
    snapshots_path = SNAPSHOT_STORE / name
    snapshots_path.mkdir(exist_ok=True, parents=True)

    create_snapshot(volume, snapshots_path / timestamp)
    click.echo(
        f"Snapshot created as: '{click.style(name, fg="green", bold=True)}/{click.style(timestamp, fg="blue")}'"
    )


@cli.command()
@arg_volume(required=False, exists=False)
def list(volume: Path):
    """List all snapshots."""
    if volume is None:
        click.echo("Listing all snapshots...")
        volume_snapshots = {
            unescape(name.relative_to(SNAPSHOT_STORE)): get_snapshots(
                name, nosnapshots_ok=True
            )
            for name in SNAPSHOT_STORE.iterdir()
            if name.is_dir()
        }
    else:
        volume, name = resolve_volume(volume)
        volume_snapshots = {unescape(name): get_snapshots(name)}
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
@arg_volume(exists=False)
@click.option(
    "-n",
    "--dry-run",
    is_flag=True,
    help="Print what would be done without deleting snapshots",
)
@click.option("-k", "--keep", type=int, help="Number of lastest snapshots to keep")
@click.option("-b", "--before", type=_DateTime(), help="Delete snapshots before date")
@click.option("-a", "--all", is_flag=True, help="Delete all snapshots")
def delete(volume: Path, before: datetime, dry_run: bool, keep: int, all: bool):
    """Delete snapshots."""
    volume, name = resolve_volume(volume)
    snapshots_path = SNAPSHOT_STORE / name
    snapshots = sorted(get_snapshots(name, nosnapshots_ok=True))

    if not all:
        filtered = False
        if keep is not None:
            filtered = True
            snapshots = snapshots[:-keep]
        if before is not None:
            filtered = True
            b = before.strftime(DATETIME_FORMAT)
            snapshots = [s for s in snapshots if s < b]
        if not filtered:
            raise click.UsageError(
                "either restrict deletion with the --keep and --before flags, or use"
                " --all to explicitly delete all snapshots"
            )
    if dry_run:
        click.echo("Dry run, no snapshots will be deleted...")
    else:
        click.echo("Deleting snapshots...")

    name_s = click.style(name, fg="green", bold=True)
    for s in snapshots:
        if not dry_run:
            delete_subvolume(snapshots_path / s)
        click.echo(f"Deleted snapshot: '{name_s}/{click.style(s, fg="blue")}'")
    if all:
        if not dry_run:
            snapshots_path.rmdir()
        click.echo(f"Removed snapshots dir for subvolume {name_s}")


def get_snapshots(name: str, nosnapshots_ok=False):
    snapshots_path = SNAPSHOT_STORE / name
    if not snapshots_path.exists():
        raise SubvolumeNotFound(unescape(name))

    snapshots = [s.name for s in snapshots_path.iterdir() if s.is_dir()]
    if not nosnapshots_ok and len(snapshots) == 0:
        raise NoSnapshotsError(unescape(name))

    return snapshots


def resolve_volume(volume: Path, exists=False) -> tuple[Path, str]:
    """Resolve volume
    Relative volume path is interepted as relative path to HOME if it cannot be
    found in current directory

    Args:
        exists: check that the resolved path exists and is a btrfs subvolume

    Returns:
        absolute path of volume and its escaped name
    """

    if not volume.is_absolute() and not volume.exists():
        volume = ROOT / volume
    volume = volume.resolve()
    if exists:
        if not volume.exists():
            raise SubvolumeNotFound(volume)
        if not btrfsutil.is_subvolume(str(volume)):
            raise NotASubvolume(volume)

    return volume, escape(volume.relative_to(ROOT))


class NotASubvolume(click.BadParameter):
    def __init__(self, path: str) -> None:
        super().__init__(f"'{path}' is not a btrfs subvolume.", param_hint="VOLUME")


class SubvolumeNotFound(click.BadParameter):
    def __init__(self, path: str) -> None:
        super().__init__(f"Subvolume '{path}' not found.", param_hint="VOLUME")


class NoSnapshotsError(click.BadParameter):
    def __init__(self, name: str) -> None:
        super().__init__(f"'{name}' does not have snapshots.", param_hint="VOLUME")


def create_snapshot(src: str, dst: str):
    try:
        btrfsutil.create_snapshot(str(src), str(dst), read_only=True)
    except BtrfsUtilError as e:
        raise click.ClickException(e)


def delete_subvolume(src: str):
    src = str(src)
    try:
        btrfsutil.set_subvolume_read_only(src, read_only=False)
        btrfsutil.delete_subvolume(src)
    except BtrfsUtilError as e:
        raise click.ClickException(e)


def escape(path: str) -> str:
    return (
        str(path).strip("/").replace(r"%", r"%%").replace("@", r"%t").replace("/", "@")
    )


def unescape(path: str) -> str:
    return str(path).replace("@", "/").replace("%t", "@").replace(r"%%", "%")


if __name__ == "__main__":
    try:
        cli()
    except FileNotFoundError as e:
        print(e)
