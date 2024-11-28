from typing import Any, List
import click

from sot import btrfs


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
