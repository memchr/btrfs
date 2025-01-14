import time
from typing import Any, List
import click

from sot import btrfs
from sot.utils import ensure_path


class Volume(click.ParamType):
    name = "volume"

    def __init__(self, exists=False, has_snapshots=False) -> None:
        self.exists = exists
        self.has_snapshots = has_snapshots
        super().__init__()

    def convert(
        self, value: Any, param: click.Parameter | None, ctx: click.Context | None
    ) -> Any:
        if isinstance(value, btrfs.Volume):
            return value
        try:
            path = ensure_path(value)
            if path.exists() and path.is_dir():
                path = path.resolve()
            volume = btrfs.Volume(path, exists=self.exists)
            if self.has_snapshots:
                volume.assert_has_snapshots()
            return volume
        except (
            btrfs.NotASubvolume,
            btrfs.SubvolumeNotFound,
            btrfs.NoSnapshotsError,
        ) as e:
            self.fail(e, param, ctx)

    def shell_complete(
        self, ctx: click.Context, param: click.Parameter, incomplete: str
    ) -> List[click.shell_completion.CompletionItem]:
        from click.shell_completion import CompletionItem

        if ctx.command.name not in ("create", "rm"):
            return [CompletionItem(v.path) for v in btrfs.STORAGE.volumes()]
        return [CompletionItem(incomplete, type="dir")]


def volume(exists=True, has_snapshots=False, **kwargs):
    return click.argument(
        "volume",
        type=Volume(exists=exists, has_snapshots=has_snapshots),
        **kwargs,
    )


class Snapshot(click.ParamType):
    name = "snapshot"

    def __init__(self, exists=False) -> None:
        self.exists = exists
        super().__init__()

    def convert(
        self, value: Any, param: click.Parameter | None, ctx: click.Context | None
    ) -> Any:
        if isinstance(value, btrfs.Snapshot):
            return value
        try:
            volume = ctx.params["volume"]
            force = ctx.params.get("force", False)
            snapshot = btrfs.Snapshot(name=value, volume=volume)
            if self.exists:
                btrfs.STORAGE.load(snapshot)
            else:
                snapshot.time = time.time()
                if not force:
                    snapshot.assert_not_exists()
            return snapshot
        except (
            btrfs.NotASubvolume,
            btrfs.SnapshotExists,
            btrfs.SubvolumeNotFound,
            btrfs.SnapshotNotFound,
            btrfs.NoSnapshotsError,
        ) as e:
            self.fail(e, param, ctx)

    def shell_complete(
        self, ctx: click.Context, param: click.Parameter, incomplete: str
    ) -> List[click.shell_completion.CompletionItem]:
        from click.shell_completion import CompletionItem

        volume: btrfs.Volume = ctx.params["volume"]
        return [CompletionItem(n) for n in volume.snapshots.keys()]


def snapshot(decl="snapshot", exists=True, nargs=1, required=True, new=False, **kwargs):
    if new:
        kwargs.setdefault("default", btrfs.Snapshot.generate_name() if nargs != -1 else None)
        required = False
        exists = False

    return click.argument(
        decl,
        type=Snapshot(exists=exists),
        nargs=nargs,
        required=required,
        **kwargs,
    )
