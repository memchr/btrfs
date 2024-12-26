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
    rebuild_database,
)
from sot import args
from sot import utils
from sot.config import MAX_COLUMNS, PAD_SNAPSHOT_NAME


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
    SnapshotStorage.open(root)


@cli.command()
def init():
    """Initialize snapshot storage"""


@cli.command()
@args.volume(exists=True)
@args.snapshot(new=True)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    help="Replace existing snapshot with the same name",
)
@click.option(
    "-m",
    "--annotation",
    type=str,
    default=None,
    help="Annotation for the snapshot",
)
@click.option("-e", "edit_annotation", is_flag=True, help="Edit annotation in $EDITOR")
def create(
    volume: Volume,
    snapshot: Snapshot,
    force: bool,
    annotation: str,
    edit_annotation: bool,
):
    """Create new snapshot."""
    if force and snapshot.name in volume.snapshots:
        volume.snapshots[snapshot.name].delete()
    if edit_annotation:
        annotation = utils.edit_annotation(annotation)
    snapshot._annotation = annotation
    snapshot.create()
    click.echo(
        f"Snapshot '{click.style(volume.name, fg='green', bold=True)}/{click.style(snapshot.name, fg='blue')}' created"
    )


@cli.command(name="list")
@args.volume(required=False, exists=False, has_snapshots=True)
@click.option("-v", "--volume-only", is_flag=True, help="List only volumes")
def list_(volume: Volume, volume_only: bool):
    """List all snapshots."""
    volumes_snapshots: dict[Volume, dict[str, Snapshot]]

    if volume_only:
        click.echo("Listing all volumes...")
        for v in Volume.all():
            head = f"  {styled(v.head)}"
            click.echo(f"{styled(v)}{head}")
        return
    elif volume is None:
        click.echo("Listing all snapshots...")
        volumes_snapshots = {v: v.snapshots for v in Volume.all()}
    else:
        volumes_snapshots = {volume: volume.snapshots}

    maxpad = min(shutil.get_terminal_size().columns - 24, MAX_COLUMNS)
    for volume, snapshots in volumes_snapshots.items():
        click.echo(styled(volume))
        for snapshot in snapshots.values():
            pad = maxpad - len(snapshot.name)
            annotation = ""
            if snapshot.annotation is not None:
                annotation = f"{" " * (PAD_SNAPSHOT_NAME - len(snapshot.name))}({snapshot.annotation})"
                pad -= len(annotation)
            click.echo(f"  {styled(snapshot)}{annotation}{" "*pad} {click.style(snapshot.strtime, fg='cyan')}")


class _DateTime(click.DateTime):
    @override
    def convert(self, value, *args, **kwargs) -> Any:
        if value == "today":
            return date.today()
        return super().convert(value, *args, **kwargs)


@cli.command()
@args.volume(exists=False, has_snapshots=True)
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
            snapshots = [
                s for s in volume.snapshots.values() if s.time < before.timestamp()
            ]
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
            volume.remove_storage()
            click.echo(f"Removed snapshots dir for subvolume {vol_styled}")
        else:
            click.echo(f"Would remove snapshots dir for subvolume {vol_styled}")


@cli.command()
@args.volume()
@args.snapshot()
def path(volume, snapshot: Snapshot):
    """Print absolute path of snapshot"""
    print(snapshot.path.resolve())


@cli.command()
def rebuild_db():
    """Rebuild the database from .sot storage and recover creation times if possible."""
    rebuild_database()
    click.echo("Database rebuilt successfully.")


@cli.command()
@args.volume(exists=False)
@args.snapshot()
@click.argument("new_annotation", required=False, type=str)
def annotate(volume: Volume, snapshot: Snapshot, new_annotation: str):
    """Annotate snapshot"""
    if new_annotation is None:
        new_annotation = utils.edit_annotation(snapshot.annotation)
    snapshot.annotation = new_annotation
    click.echo(f"Snapshot '{styled(snapshot)}' annotated with: {new_annotation}")


@cli.command()
@args.volume(exists=False)
@args.snapshot()
@click.argument("workdir", type=click.Path(file_okay=False, path_type=Path))
def load(volume: Volume, snapshot: Snapshot, workdir: Path):
    """Create a read-write snapshot of snapshot to workdir."""
    if workdir.exists():
        raise click.UsageError(f"Workdir '{workdir}' already exists")
    snapshot.load_to_path(workdir)
    click.echo(
        f"Snapshot '{styled(snapshot)}' loaded to '{click.style(str(workdir), fg='green')}'"
    )


@cli.command()
@args.volume(exists=False, has_snapshots=True)
@args.snapshot(required=False)
def switch(volume: Volume, snapshot: Snapshot):
    """Switch volume to snapshot."""
    volume.switch(snapshot)
    click.echo(f"Volume '{styled(volume)}' switched to snapshot '{styled(snapshot)}'")


@cli.command()
@args.volume(exists=True)
def rm(volume: Volume):
    """Remove arbitrary volume"""
    volume.delete()


def styled(obj: Snapshot | Volume) -> str:
    if obj is None:
        return ""
    if isinstance(obj, Snapshot):
        if obj.is_head():
            bold = True
        else:
            bold = False
        return click.style(obj.name, fg="yellow", bold=bold)
    elif isinstance(obj, Volume):
        return click.style(obj.path, fg="green", bold=True)


def main():
    # Workaround: click doesn't call cli() in completion mode, so we need to set
    # btrfs.STORAGE here
    if "_SOT_COMPLETE" in os.environ:
        SnapshotStorage.open()
    try:
        cli()
    finally:
        SnapshotStorage.close()
