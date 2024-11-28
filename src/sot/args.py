import time
from typing import Any, List
import click

from sot import btrfs
from sot.btrfs import config


class Volume(click.ParamType):
    name = "volume"

    def __init__(self, exists=False) -> None:
        self.exists = exists
        super().__init__()

    def convert(
        self, value: Any, param: click.Parameter | None, ctx: click.Context | None
    ) -> Any:
        if isinstance(value, btrfs.Volume):
            return value
        try:
            return btrfs.Volume(value, exists=self.exists)
        except (btrfs.NotASubvolume, btrfs.SubvolumeNotFound) as e:
            self.fail(e, param, ctx)

    def shell_complete(
        self, ctx: click.Context, param: click.Parameter, incomplete: str
    ) -> List[click.shell_completion.CompletionItem]:
        from click.shell_completion import CompletionItem

        return [CompletionItem(incomplete, type="dir")]


def volume(required=True, exists=True):
    return click.argument(
        "volume",
        type=Volume(exists=exists),
        required=required,
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
            if self.exists:
                return config.STORAGE.query(btrfs.Snapshot(name=value, volume=volume))
            else:
                return btrfs.Snapshot(name=value, volume=volume, time=time.time())
        except (btrfs.NotASubvolume, btrfs.SubvolumeNotFound) as e:
            self.fail(e, param, ctx)

    # def shell_complete(
    #     self, ctx: click.Context, param: click.Parameter, incomplete: str
    # ) -> List[click.shell_completion.CompletionItem]:
    #     from click.shell_completion import CompletionItem

    #     return [CompletionItem(incomplete, type="dir")]


def snapshot(required=True, exists=True):
    return click.argument(
        "snapshot",
        type=Snapshot(exists=exists),
        required=required,
        default=btrfs.Snapshot.generate_name,
    )
