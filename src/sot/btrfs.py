from __future__ import annotations
from datetime import datetime
import hashlib
import os
from pathlib import Path
import shutil
import btrfsutil
import sqlite3

from sot.utils import ensure_path, escape, unescape
from sot import config


STORAGE: SnapshotStorage = None


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
        self.root: Path = root
        self.path: Path = root / config.SNAPSHOT_DIR
        self._db = self.path / "index.db"
        self._conn = sqlite3.connect(self._db)
        self._conn.row_factory = sqlite3.Row
        self._cur = self._conn.cursor()
        self._init_db()

    def _init_db(self):
        with self._conn:
            self._cur.execute("PRAGMA foreign_keys = ON")
        self._create_tables()

    def _create_tables(self):
        with self._conn:
            self._cur.execute("""
                CREATE TABLE IF NOT EXISTS volumes (
                    id INTEGER PRIMARY KEY,
                    path TEXT UNIQUE
                )
            """)
            self._cur.execute("""
                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY,
                    volume_id INTEGER NOT NULL,
                    name TEXT,
                    time REAL,
                    annotation TEXT,
                    FOREIGN KEY (volume_id) REFERENCES volumes (id) ON DELETE CASCADE,
                    UNIQUE (volume_id, name)
                )
            """)
            # Stores the "HEAD" of the volume, i.e. the last check-out snapshot, if there is one.
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS volumes_head (
                    volume_id INTEGER PRIMARY KEY,
                    head_snapshot_id INTEGER,
                    FOREIGN KEY (volume_id) REFERENCES volumes (id) ON DELETE CASCADE,
                    FOREIGN KEY (head_snapshot_id) REFERENCES snapshots (id) ON DELETE SET NULL
                )
            """)
            self._cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_volume_path ON volumes (path)"
            )
            self._cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_snapshot_volume_id ON snapshots (volume_id)"
            )

    def load(self, obj: "Snapshot" | "Volume", force=False):
        # object is already loaded
        if obj.id is not None and not force:
            return

        if isinstance(obj, Volume):
            with self._conn:
                row = self._cur.execute(
                    "SELECT id FROM volumes WHERE path = ?", (str(obj.path),)
                ).fetchone()
                if row is not None:
                    obj.id = row["id"]
        if isinstance(obj, Snapshot):
            self.load(obj.volume)
            with self._conn:
                row = self._cur.execute(
                    "SELECT id, time, annotation FROM snapshots WHERE volume_id = ? AND name = ?",
                    (obj.volume.id, obj.name),
                ).fetchone()
                if row is None:
                    raise SnapshotNotFound(obj)
                obj.time = row["time"]
                obj.id = row["id"]
                obj.annotation = row["annotation"]

    def register(self, obj: "Snapshot" | "Volume"):
        if isinstance(obj, Snapshot):
            self.register(obj.volume)
            self.load(obj.volume)

            with self._conn:
                self._cur.execute(
                    "INSERT OR REPLACE INTO snapshots (volume_id, name, time, annotation) VALUES (?, ?, ?, ?)",
                    (obj.volume.id, obj.name, obj.time, obj.annotation),
                )
                if self._cur.rowcount > 0:
                    obj.id = self._cur.lastrowid
        elif isinstance(obj, Volume):
            with self._conn:
                self._cur.execute(
                    "INSERT OR IGNORE INTO volumes (path) VALUES (?)", (str(obj.path),)
                )
                if self._cur.rowcount > 0:
                    obj.id = self._cur.lastrowid
                    self._cur.execute(
                        "INSERT OR IGNORE INTO volumes_head (volume_id) VALUES (?)",
                        (obj.id,),
                    )

    def unregister(self, obj: "Snapshot" | "Volume"):
        if isinstance(obj, Snapshot):
            self.load(obj.volume)

            with self._conn:
                self._cur.execute(
                    "DELETE FROM snapshots WHERE volume_id = ? AND (name = ? OR id = ?)",
                    (obj.volume.id, obj.name, obj.id),
                )
        elif isinstance(obj, Volume):
            with self._conn:
                if obj.id is None:
                    return

                self._cur.execute("DELETE FROM volumes WHERE id = ?", (obj.id,))

    def snapshots(self, volume: "Volume") -> dict[str, Snapshot]:
        self.load(volume)
        with self._conn:
            rows = self._cur.execute(
                "SELECT id, name, time, annotation FROM snapshots WHERE volume_id = ? ORDER BY time DESC",
                (volume.id,),
            ).fetchall()
        return {
            row["name"]: Snapshot(
                volume=volume,
                id=row["id"],
                name=row["name"],
                time=row["time"],
                annotation=row["annotation"],
            )
            for row in rows
        }

    def volumes(self):
        with self._conn:
            rows = self._cur.execute("SELECT id, path FROM volumes").fetchall()
        for id, path in rows:
            yield Volume(path=path, id=id)

    def head(self, volume: Volume) -> Snapshot | None:
        self.load(volume)
        with self._conn:
            row = self._cur.execute("""
                SELECT id, name, time, annotation, head_snapshot_id
                FROM snapshots
                JOIN volumes_head ON snapshots.id = head_snapshot_id
                WHERE snapshots.volume_id = ?
            """, (volume.id,)).fetchone()
        if row is None:
            return None
        return Snapshot(
            volume=volume,
            id=row["id"],
            name=row["name"],
            time=row["time"],
            annotation=row["annotation"],
        )

    def set_head(self, volume: Volume, snapshot: Snapshot):
        self.load(volume)
        self.load(snapshot)
        with self._conn:
            self._cur.execute(
                "UPDATE volumes_head SET head_snapshot_id = ? WHERE volume_id = ?",
                (snapshot.id, volume.id),
            )

    def rebuild_database(self):
        """Rebuild the database from .sot storage and recover creation times if possible."""

        # drop existing tables and indexes
        with self._conn:
            self._cur.execute("DROP TABLE IF EXISTS volumes")
            self._cur.execute("DROP TABLE IF EXISTS snapshots")
            self._cur.execute("DROP TABLE IF EXISTS volumes_head")
            self._cur.execute("DROP INDEX IF EXISTS idx_volume_path")
            self._cur.execute("DROP INDEX IF EXISTS idx_snapshot_volume_id")

        self._create_tables()
        for volume in self.volumes_from_filesystem():
            self.register(volume)
            for snapshot in self.snapshots_from_filesystem(volume):
                self.register(snapshot)

    def volumes_from_filesystem(self):
        """Yield volumes found in the filesystem."""
        for volume_path in self.path.iterdir():
            if volume_path.is_dir():
                yield Volume(path=unescape(volume_path.name))

    def snapshots_from_filesystem(self, volume: Volume):
        """Yield snapshots found in the filesystem for a given volume."""
        for snapshot_path in volume.storage.iterdir():
            if snapshot_path.is_dir():
                snapshot = Snapshot(volume, snapshot_path.name)
                snapshot.time = snapshot_path.stat().st_ctime
                yield snapshot

    def __del__(self):
        self._cur.close()
        self._conn.close()


class Volume:
    def __init__(self, path: Path = None, exists=False, id: int = None) -> None:
        """
        Arguments:
            id {int} -- Volume ID in the database
            path {Path} -- Path to the volume, relative to the storage root
            exists {bool} -- Check if the volume exists
        """
        self.path: Path = ensure_path(path)
        self.id: int | None = id
        # escape volume name
        self.name: str = escape(self.path)
        # path of volume in the filesystem
        self.realpath: Path = STORAGE.root / self.path
        # subvolume storage path
        self.storage: Path = STORAGE.path / self.name

        if exists:
            self.assert_is_volume()

    def remove_storage(self):
        self.storage.rmdir()
        STORAGE.unregister(self)

    def switch(self, snapshot: Snapshot):
        """Switch volume to snapshot."""
        self.assert_is_volume()
        self.assert_has_snapshots()

        if snapshot.name not in self.snapshots:
            raise SnapshotNotFound(snapshot)

        # volume doesn't exist, just load the snapshot
        if not self.realpath.exists():
            snapshot.load_to_path(self.realpath)
            return snapshot

        # volume path exist but is not a subvolume
        if not btrfsutil.is_subvolume(str(self.realpath)):
            raise NotASubvolume(self.realpath)

        self.delete()
        snapshot.load_to_path(self.realpath)

        # set snapshot as head of the volume
        STORAGE.set_head(self, snapshot)

    def delete(self):
        btrfsutil.delete_subvolume(str(self.realpath))

    @property
    def snapshots(self) -> dict[str, "Snapshot"]:
        return STORAGE.snapshots(self)

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
    def __init__(
        self,
        volume: Volume,
        name: str,
        time: float = 0,
        id: int = None,
        annotation: str = None,
    ) -> None:
        if name is None:
            while (volume.storage / (name := self.generate_name())).exists():
                pass

        self.volume: Volume = volume
        self.path: Path
        self._name = name
        self.path: Path = self.volume.storage / self.name
        self.time: float = time
        self.id: int | None = id
        self.annotation: str | None = annotation

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, new_name):
        if new_name in self.volume.snapshots:
            raise SnapshotExists(new_name)

        STORAGE.unregister(self)
        self._name = new_name
        self.readonly = False
        old_path = self.path
        self.path = self.volume.storage / self._name
        shutil.move(old_path, self.path)
        STORAGE.register(self)

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
        STORAGE.register(self)
        # set snapshot as head of the volume
        STORAGE.set_head(self.volume, self)

    def delete(self) -> None:
        path = str(self.path)
        self.readonly = False
        btrfsutil.delete_subvolume(path)
        STORAGE.unregister(self)

    def load_to_path(self, workdir: Path):
        """Create a read-write snapshot of snapshot to workdir."""
        btrfsutil.create_snapshot(str(self.path), str(workdir), read_only=False)

    def is_head(self) -> bool:
        return STORAGE.head(self.volume).id == self.id

    @property
    def strtime(self):
        return datetime.fromtimestamp(self.time).strftime(config.DATETIME_FORMAT)

    @staticmethod
    def generate_name():
        return hashlib.sha256(os.urandom(16)).hexdigest()[:7]

    def assert_not_exists(self):
        if self.path.exists():
            raise SnapshotExists(self)

    def __repr__(self) -> str:
        return self.name
