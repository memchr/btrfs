#!/usr/bin/python
from __future__ import annotations
from datetime import date, datetime
import os
from pathlib import Path
import shutil
from typing import Any, override
from btrfsutil import BtrfsUtilError
import click
import click.shell_completion

from sot.btrfs import (
    Snapshot,
    SnapshotExists,
    SnapshotStorage,
    Volume,
    config,
)
from sot import args


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option(
    "-r",
    "--root",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="btrfs root",
)
def cli(root: Path):
    """BTRFS snapshots management."""
    config.STORAGE = SnapshotStorage(root)


@cli.command()
def init():
    """Initialize snapshot storage"""


@cli.command()
@args.volume(exists=True)
@args.snapshot(exists=False)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    help="Replace existing snapshot with the same name",
)
def create(volume: Volume, snapshot: Snapshot, force: bool):
    """Create new snapshot."""
    if force and snapshot.name in volume.snapshots:
        volume.snapshots[snapshot.name].delete()
    snapshot.create()
    click.echo(
        f"Snapshot '{click.style(volume.name, fg='green', bold=True)}/{click.style(snapshot.name, fg='blue')}' created"
    )


@cli.command(name="list")
@args.volume(required=False, exists=False, has_snapshots=True)
def list_(volume: Volume):
    """List all snapshots."""
    volumes_snapshots: dict[Volume, dict[str, Snapshot]]

    if volume is None:
        click.echo("Listing all snapshots...")
        volumes_snapshots = {
            (v := Volume(name=d.name)): v.snapshots for d in config.STORAGE.iter()
        }
    else:
        volumes_snapshots = {volume: volume.snapshots}

    leftpad = min(shutil.get_terminal_size().columns - 16, 60)
    for volume, snapshots in volumes_snapshots.items():
        click.echo(styled(volume))
        for snapshot in snapshots.values():
            click.echo(f"  {styled(snapshot):<{leftpad}} {snapshot.strtime}")


class _DateTime(click.DateTime):
    @override
    def convert(self, value, *args, **kwargs) -> Any:
        if value == "today":
            return date.today()
        return super().convert(value, *args, **kwargs)


@cli.command()
@args.volume(exists=False)
@args.snapshot()
@click.argument("name", type=click.STRING)
def rename(volume: Volume, snapshot: Snapshot, name: str):
    """Rename snapshot"""
    try:
        old = styled(snapshot)
        snapshot.name = name
        click.echo(f"Renamed Snapshot {old} to {styled(snapshot)}")
    except SnapshotExists as e:
        raise click.UsageError(f"Cannot rename 'f{snapshot.name}' to '{name}': {e}")


@cli.command()
@args.volume(exists=False, has_snapshots=True)
@args.snapshot("snapshots", required=False, nargs=-1)
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
            snapshots = volume.snapshots.values()
        elif keep is not None:
            snapshots = volume.snapshots.values()[:-keep]
        elif before is not None:
            raise NotImplementedError
            # b = before.strftime(DATETIME_FORMAT)
            # snapshots = [name for name in volume.snapshots if name < b]
        if len(snapshots) == 0:
            raise click.UsageError("No snapshots available for deletion.")
    if dry_run:
        click.echo("Dry run, no snapshots will be deleted...")
    else:
        click.echo("Deleting snapshots...")

    vol_styled = styled(volume)
    for s in snapshots:
        if not dry_run:
            try:
                s.delete()
                click.echo(f"Deleted snapshot: '{vol_styled}/{styled(s)}'")
            except BtrfsUtilError as e:
                click.echo(f"Error: {e.strerror}: {e.filename}", err=True)
            except Warning as e:
                click.echo(f"Warning: {e}", err=True)
        else:
            click.echo(f"Would delete: '{vol_styled}/{styled(s)}'")
    if all or len(volume.snapshots) == 0:
        if not dry_run:
            volume.snapshots_path.rmdir()
            click.echo(f"Removed snapshots dir for subvolume {vol_styled}")
        else:
            click.echo(f"Would remove snapshots dir for subvolume {vol_styled}")


@cli.command()
@args.volume()
@args.snapshot()
def path(volume, snapshot: Snapshot):
    """Print absolute path of snapshot"""
    print(snapshot.path.resolve())


def styled(obj: Snapshot | Volume) -> str:
    if isinstance(obj, Snapshot):
        return click.style(obj.name, fg="yellow")
    elif isinstance(obj, Volume):
        return click.style(obj.relative_path, fg="green", bold=True)


def main():
    # Workaround: click doesn't call cli() in completion mode, so we need to set
    # config.STORAGE here
    if "_SOT_COMPLETE" in os.environ:
        config.STORAGE = SnapshotStorage()
    cli()
