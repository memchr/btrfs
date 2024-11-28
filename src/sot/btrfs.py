from __future__ import annotations
from datetime import datetime
import hashlib
import os
from pathlib import Path
import btrfsutil
import json

from sot.utils import ensure_path, escape, unescape


class config:
    STORAGE: "SnapshotStorage" = None
    DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
    SNAPSHOT_DIR = ".sot"


class NotASubvolume(ValueError):
    def __init__(self, path: str) -> None:
        super().__init__(f"'{path}' is not a btrfs subvolume.")


class SubvolumeNotFound(FileNotFoundError):
    def __init__(self, volume) -> None:
        super().__init__(f"Subvolume '{volume}' not found.")


class SnapshotNotFound(FileNotFoundError):
    def __init__(self, snapshot) -> None:
        super().__init__(f"Snapshot '{snapshot}' not found.")


class SnapshotExists(FileExistsError):
    def __init__(self, snapshot) -> None:
        super().__init__(f"Snapshot '{snapshot}' exists.")


class NoSnapshotsError(FileNotFoundError):
    def __init__(self, volume) -> None:
        super().__init__(f"'{volume}' does not have snapshots.")


class SnapshotStorage:
    MetadataType = dict[str, dict[str, float]]

    def __init__(self, root: Path | None = None) -> None:
        if root is None:
            root = Path.cwd()
            while True:
                if (root / ".sot").is_dir():
                    break
                root = root.parent
        else:
            root = ensure_path(root).resolve()
        self.root = root
        self.path = root / config.SNAPSHOT_DIR
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
                md[volume.name][obj.name] = obj.time
            else:
                md[volume.name] = {obj.name: obj.time}
            self._metadata = md
        elif isinstance(obj, Volume):
            raise NotImplementedError

    def query(self, obj: "Snapshot" | "Volume") -> "Snapshot" | "Volume":
        if isinstance(obj, Snapshot):
            try:
                volume = self._metadata_cached[obj.volume.name]
            except KeyError:
                raise SubvolumeNotFound(obj.volume)
            try:
                obj.time = volume[obj.name]
                return obj
            except KeyError:
                raise SnapshotNotFound(obj)

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
    def __init__(self, path: Path = None, name=None, exists=False) -> None:
        """
        Relative volume path is interepted as relative path to SubvolumeStorage
        if it cannot be found in current directory
        """
        path = ensure_path(path if name is None else unescape(name))
        self.path = path.resolve() if path.exists() else config.STORAGE.root / path
        self.relative_path = self.path.relative_to(config.STORAGE.root)
        self.name = escape(self.relative_path)
        self.snapshots_path = config.STORAGE.path / self.name

        if exists:
            self.assert_is_volume()

    def assert_is_volume(self):
        path = self.path
        if not btrfsutil.is_subvolume(str(path)):
            raise NotASubvolume(path)
        if not path.exists():
            raise SubvolumeNotFound(path)

    def assert_has_snapshots(self):
        if not self.snapshots_path.exists():
            raise NoSnapshotsError(self)

    @property
    def snapshots(self) -> list["Snapshot"]:
        path = self.snapshots_path
        if not path.exists():
            return []
        return config.STORAGE.query(self)

    def __repr__(self) -> str:
        return str(self.path)


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
            raise SnapshotExists(self)
        self.path.parent.mkdir(exist_ok=True, parents=True)
        btrfsutil.create_snapshot(str(self.volume.path), str(self.path), read_only=True)
        config.STORAGE.register(self)

    def delete(self) -> None:
        path = str(self.path)
        btrfsutil.set_subvolume_read_only(path, read_only=False)
        btrfsutil.delete_subvolume(path)
        config.STORAGE.unregister(self)
        pass

    @property
    def strtime(self):
        return datetime.fromtimestamp(self.time).strftime(config.DATETIME_FORMAT)

    @staticmethod
    def generate_name():
        return hashlib.sha256(os.urandom(16)).hexdigest()[:8]

    def __repr__(self) -> str:
        return self.name
