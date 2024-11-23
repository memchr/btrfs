#!/usr/bin/python
from __future__ import annotations
from datetime import date, datetime
from pathlib import Path
import shutil
from typing import Any, override
from btrfsutil import BtrfsUtilError
import click

from sot.btrfs import (
    NotASubvolume,
    Snapshot,
    SnapshotStorage,
    SubvolumeNotFound,
    Volume,
    config,
)


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


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
    config.STORAGE = SnapshotStorage(root if root is not None else Path.home())


@cli.command()
def init():
    """Initialize snapshot storage"""


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
            for d in config.STORAGE.iter()
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


def main():
    try:
        cli()
    except FileNotFoundError as e:
        print(e)
