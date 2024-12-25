from __future__ import annotations
from datetime import datetime
import hashlib
import os
from pathlib import Path
import shutil
import btrfsutil
import sqlite3

from sot.utils import ensure_path, escape


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
        super().__init__(f"Subvolume '{volume}' does not have snapshots.")


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
        self._db = self.path / "index.db"
        self._conn = sqlite3.connect(self._db)
        self._conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self):
        with self._conn:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS volumes (
                    id INTEGER PRIMARY KEY,
                    path TEXT UNIQUE
                )
            """)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY,
                    volume_id INTEGER NOT NULL,
                    name TEXT,
                    time REAL,
                    FOREIGN KEY (volume_id) REFERENCES volumes (id),
                    UNIQUE (volume_id, name)
                )
            """)
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_volume_path ON volumes (path)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_snapshot_volume_id ON snapshots (volume_id)"
            )

    def load(self, obj: "Snapshot" | "Volume", force = False):
        # object is already loaded
        if obj.id is not None and not force:
            return

        if isinstance(obj, Volume):

            with self._conn:
                row = self._conn.execute(
                    "SELECT id FROM volumes WHERE path = ?", (str(obj.path),)
                ).fetchone()
                if row is not None:
                    obj.id = row["id"]
        if isinstance(obj, Snapshot):
            self.load(obj.volume)
            with self._conn:
                row = self._conn.execute(
                    "SELECT id, time FROM snapshots WHERE volume_id = ? AND name = ?",
                    (obj.volume.id, obj.name),
                ).fetchone()
                if row is None:
                    raise SnapshotNotFound(obj)
                obj.time = row["time"]
                obj.id = row["id"]

    def register(self, obj: "Snapshot" | "Volume"):
        if isinstance(obj, Snapshot):
            self.register(obj.volume)
            self.load(obj.volume)

            with self._conn:
                self._conn.execute(
                    "INSERT OR REPLACE INTO snapshots (volume_id, name, time) VALUES (?, ?, ?)",
                    (obj.volume.id, obj.name, obj.time),
                )
        elif isinstance(obj, Volume):
            with self._conn:
                self._conn.execute(
                    "INSERT OR IGNORE INTO volumes (path) VALUES (?)", (str(obj.path),)
                )

    def unregister(self, obj: "Snapshot" | "Volume"):
        if isinstance(obj, Snapshot):
            self.load(obj.volume)

            with self._conn:
                self._conn.execute(
                    "DELETE FROM snapshots WHERE volume_id = ? AND name = ?",
                    (obj.volume.id, obj.name),
                )
        elif isinstance(obj, Volume):
            with self._conn:
                if obj.id is None:
                    return

                self._conn.execute(
                    "DELETE FROM snapshots WHERE volume_id = ?", (obj.id,)
                )
                self._conn.execute("DELETE FROM volumes WHERE id = ?", (obj.id,))

    def snapshots(self, volume: "Volume") -> dict[str, Snapshot]:
        self.load(volume)
        with self._conn:
            rows = self._conn.execute(
                "SELECT name, time FROM snapshots WHERE volume_id = ?", (volume.id,)
            ).fetchall()
        return {name: Snapshot(volume, name, time) for name, time in rows}

    def volumes(self):
        with self._conn:
            rows = self._conn.execute("SELECT id, path FROM volumes").fetchall()
        for id, path in rows:
            yield Volume(path=path, id=id)

    def __del__(self):
        self._conn.close()


class Volume:
    def __init__(self, path: Path = None, exists=False, id: int = None) -> None:
        """
        Arguments:
            id {int} -- Volume ID in the database
            path {Path} -- Path to the volume, relative to the storage root
            exists {bool} -- Check if the volume exists
        """
        self.path = ensure_path(path)
        self.id: int | None = id
        # escape volume name
        self.name = escape(self.path)
        # path of volume in the filesystem
        self.realpath = config.STORAGE.root / self.path
        # subvolume storage path
        self.storage = config.STORAGE.path / self.name

        if exists:
            self.assert_is_volume()

    @property
    def snapshots(self) -> dict[str, "Snapshot"]:
        return config.STORAGE.snapshots(self)

    def assert_is_volume(self):
        path = self.realpath
        if not path.exists():
            raise SubvolumeNotFound(path)
        if not btrfsutil.is_subvolume(str(path)):
            raise NotASubvolume(path)

    def assert_has_snapshots(self):
        if not self.storage.exists():
            raise NoSnapshotsError(self)

    def __repr__(self) -> str:
        return str(self.path)


class Snapshot:
    def __init__(self, volume: Volume, name: str, time: float = 0, id: int = None) -> None:
        if name is None:
            while (volume.storage / (name := self.generate_name())).exists():
                pass

        self.volume = volume
        self.path: Path
        self._name = name
        self.path = self.volume.storage / self.name
        self.time = time
        self.id: int | None = id

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, new_name):
        if new_name in self.volume.snapshots:
            raise SnapshotExists(new_name)

        config.STORAGE.unregister(self)
        self._name = new_name
        self.readonly = False
        old_path = self.path
        self.path = self.volume.storage / self._name
        shutil.move(old_path, self.path)
        config.STORAGE.register(self)

    @property
    def readonly(self) -> bool:
        return btrfsutil.get_subvolume_read_only(self.path)

    @readonly.setter
    def readonly(self, read_only: bool):
        btrfsutil.set_subvolume_read_only(str(self.path), read_only=read_only)

    def create(self) -> None:
        self.path.parent.mkdir(exist_ok=True, parents=True)
        btrfsutil.create_snapshot(
            str(self.volume.realpath), str(self.path), read_only=True
        )
        config.STORAGE.register(self)

    def delete(self) -> None:
        path = str(self.path)
        self.readonly = False
        btrfsutil.delete_subvolume(path)
        config.STORAGE.unregister(self)

    @property
    def strtime(self):
        return datetime.fromtimestamp(self.time).strftime(config.DATETIME_FORMAT)

    @staticmethod
    def generate_name():
        return hashlib.sha256(os.urandom(16)).hexdigest()[:8]

    def assert_not_exists(self):
        if self.path.exists():
            raise SnapshotExists(self)

    def __repr__(self) -> str:
        return self.name
