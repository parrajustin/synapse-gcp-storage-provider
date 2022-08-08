
from ast import Lambda
from distutils.log import debug
from functools import cache
import attr
import logging
import sqlite3
import datetime
import os
from dataclasses import dataclass
import threading

from time import sleep
from typing import List, Literal
from twisted.python.failure import Failure
from twisted.internet import threads
from twisted.python.threadpool import ThreadPool
from google.cloud import storage
from twisted.python.threadpool import ThreadPool

from synapse.types import ISynapseReactor
from synapse.module_api import ModuleApi
from synapse.logging.context import LoggingContext

# Schema for our sqlite database cache
SCHEMA = """
    CREATE TABLE IF NOT EXISTS media (
        origin TEXT NOT NULL,  -- empty string if local media
        media_id TEXT NOT NULL,
        filesystem_id TEXT NOT NULL,
        -- Type is "local" or "remote"
        type TEXT NOT NULL,
        -- indicates whether the media and all its thumbnails have been deleted from the
        -- local cache
        known_deleted BOOLEAN NOT NULL
    );

    CREATE UNIQUE INDEX IF NOT EXISTS media_id_idx ON media(origin, media_id);
    CREATE INDEX IF NOT EXISTS deleted_idx ON media(known_deleted);
"""

logger = logging.getLogger(__name__)

# @dataclass
# class GcpUpdaterModuleConfig:
#     key_path: str
#     bucket: str
#     # Duration afterwhich items are deleted, a string with supports suffix of 's', 'm', 'h', 'd', 'M' or 'y'.
#     duration: str
#     # Max threadpool size.
#     threadpool_size: int
#     # Cache db absolute location.
#     cache_db: str
#     # Synapse sqlite3 path
#     homserver_db: str
#     # Number of seconds to sleep.
#     sleep_secs: float

def _update_db_process_rows(mtype: Literal['local', 'remote'], sqlite_cur: sqlite3.Cursor, synapse_db_curs: sqlite3.Cursor):
    """Process rows extracted from Synapse's database and insert them in cache"""
    update_count = 0

    for (origin, media_id, filesystem_id) in synapse_db_curs:
        sqlite_cur.execute(
            """
            INSERT OR IGNORE INTO media
            (origin, media_id, filesystem_id, type, known_deleted)
            VALUES (?, ?, ?, ?, ?)
            """,
            (origin, media_id, filesystem_id, mtype, False),
        )
        update_count += sqlite_cur.rowcount

    return update_count

def _run_update_db(synapse_db_conn: sqlite3.Connection, sqlite_conn: sqlite3.Connection, before_date: datetime.datetime):
    """Entry point for update-db command
    """

    local_sql = """
        SELECT '', media_id, media_id
        FROM local_media_repository
        WHERE
            COALESCE(last_access_ts, created_ts) < %s
            AND url_cache IS NULL
    """
    remote_sql = """
        SELECT media_origin, media_id, filesystem_id
        FROM remote_media_cache
        WHERE
            COALESCE(last_access_ts, created_ts) < %s
    """
    last_access_ts = int(before_date.timestamp() * 1000)
    logger.info("[GCP][UPDATER] Syncing files that haven't been accessed since: %s", before_date.isoformat(" "))
    update_count = 0

    with sqlite_conn:
        sqlite_cur = sqlite_conn.cursor()

        synapse_db_curs = synapse_db_conn.cursor()
        for sql, mtype in ((local_sql, "local"), (remote_sql, "remote")):
            synapse_db_curs.execute(sql.replace("%s", "?"), (last_access_ts,))
            update_count += _update_db_process_rows(mtype, sqlite_cur, synapse_db_curs)

    logger.info("[GCP][UPDATER] Synced %i new rows", update_count)

    synapse_db_conn.close()

def _get_not_deleted(sqlite_conn) -> sqlite3.Cursor:
    """Get all rows in our cache that we don't think have been deleted
    """
    cur = sqlite_conn.cursor()

    cur.execute(
        """
        SELECT origin, media_id, filesystem_id, type FROM media
        WHERE NOT known_deleted
        """
    )
    return cur

def _get_not_deleted_count(sqlite_conn: sqlite3.Connection) -> int:
    """Get count of all rows in our cache that we don't think have been deleted
    """
    cur = sqlite_conn.cursor()

    cur.execute(
        """
        SELECT COALESCE(count(*), 0) FROM media
        WHERE NOT known_deleted
        """
    )
    (count,) = cur.fetchone()
    return count

def _to_thumbnail_dir(origin: str, filesystem_id: str, m_type: Literal['local', 'remote']):
    """Get a relative path to the given media's thumbnail directory
    """
    if m_type == "local":
        thumbnail_path = os.path.join(
            "local_thumbnails",
            filesystem_id[:2],
            filesystem_id[2:4],
            filesystem_id[4:],
        )
    elif m_type == "remote":
        thumbnail_path = os.path.join(
            "remote_thumbnail",
            origin,
            filesystem_id[:2],
            filesystem_id[2:4],
            filesystem_id[4:],
        )
    else:
        raise Exception("Unexpected media type %r", m_type)

    return thumbnail_path

def _to_path(origin: str, filesystem_id: str, m_type: Literal['local', 'remote']):
    """Get a relative path to the given media
    """
    if m_type == "local":
        file_path = os.path.join(
            "local_content", filesystem_id[:2], filesystem_id[2:4], filesystem_id[4:],
        )
    elif m_type == "remote":
        file_path = os.path.join(
            "remote_content",
            origin,
            filesystem_id[:2],
            filesystem_id[2:4],
            filesystem_id[4:],
        )
    else:
        raise Exception("Unexpected media type %r", m_type)

    return file_path

def _get_local_files(base_path: str, origin: str, filesystem_id: str, m_type: Literal['local', 'remote']) -> List[str]:
    """Get a list of relative paths to undeleted files for the given media
    """
    local_files = []

    original_path = _to_path(origin, filesystem_id, m_type)
    if os.path.exists(os.path.join(base_path, original_path)):
        local_files.append(original_path)

    thumbnail_path = _to_thumbnail_dir(origin, filesystem_id, m_type)
    try:
        with os.scandir(os.path.join(base_path, thumbnail_path)) as dir_entries:
            for dir_entry in dir_entries:
                if dir_entry.is_file():
                    local_files.append(os.path.join(thumbnail_path, dir_entry.name))
    except FileNotFoundError:
        # The thumbnail directory does not exist
        pass
    except NotADirectoryError:
        # The thumbnail directory is not a directory for some reason
        pass

    return local_files

def _run_check_delete(sqlite_conn: sqlite3.Connection, base_path: str):
    """Entry point for check-deleted command
    """
    deleted = []
    it = _get_not_deleted(sqlite_conn)
    logger.info("[GCP][UPDATER] Checking on %s undeleted files", _get_not_deleted_count(sqlite_conn))

    for origin, media_id, filesystem_id, m_type in it:
        local_files = _get_local_files(base_path, origin, filesystem_id, m_type)
        if not local_files:
            deleted.append((origin, media_id))

    with sqlite_conn:
        sqlite_conn.executemany(
            """
            UPDATE media SET known_deleted = ?
            WHERE origin = ? AND media_id = ?
            """,
            ((True, o, m) for o, m in deleted),
        )

    logger.info("[GCP][UPDATER] Updated %i as deleted", len(deleted))

def _parse_duration(duration_str: str) -> datetime.datetime:
    """Parse a string into a duration supports suffix of d, m or y.
    """
    logger.debug("[GCP][UPDATER] Parsing %s", duration_str)
    suffix = duration_str[-1]
    number = int(duration_str[:-1])

    now = datetime.datetime.now()
    then: datetime.datetime = None
    if suffix == 's':
        then = now - datetime.timedelta(seconds=number)
    elif suffix == "m":
        then = now - datetime.timedelta(minutes=number)
    elif suffix == "h":
        then = now - datetime.timedelta(hours=number)
    elif suffix == "d":
        then = now - datetime.timedelta(days=number)
    elif suffix == "M":
        then = now - datetime.timedelta(days=30 * number)
    elif suffix == "y":
        then = now - datetime.timedelta(days=365 * number)
    else:
        raise Exception("duration must end in 's', 'm', 'h', 'd', 'M' or 'y'")

    return then

def _mark_as_deleted(sqlite_conn, origin, media_id):
    with sqlite_conn:
        sqlite_conn.execute(
            """
            UPDATE media SET known_deleted = ?
            WHERE origin = ? AND media_id = ?
            """,
            (True, origin, media_id),
        )

def _run_upload(gcp_client: storage.Client, bucket: str, sqlite_conn: sqlite3.Connection, base_path: str):
    """Entry point for upload command
    """
    total = _get_not_deleted_count(sqlite_conn)
    logger.debug("[GCP][UPDATER] Uploading %i total files.", total)

    uploaded_media = 0
    uploaded_files = 0
    uploaded_bytes = 0
    deleted_media = 0
    deleted_files = 0
    deleted_bytes = 0

    it = _get_not_deleted(sqlite_conn)
    gcp_bucket = gcp_client.bucket(bucket)

    for origin, media_id, filesystem_id, m_type in it:
        local_files = _get_local_files(base_path, origin, filesystem_id, m_type)

        if not local_files:
            _mark_as_deleted(sqlite_conn, origin, media_id)
            continue

        # Counters of uploaded and deleted files for this media only
        media_uploaded_files = 0
        media_deleted_files = 0

        for rel_file_path in local_files:
            local_path = os.path.join(base_path, rel_file_path)
            blob = gcp_bucket.blob(rel_file_path)

            if not blob.exists():
                blob.upload_from_filename(local_path)

                media_uploaded_files += 1
                uploaded_files += 1
                uploaded_bytes += os.path.getsize(local_path)

            size = os.path.getsize(local_path)
            os.remove(local_path)

            try:
                # This may have lead to an empty directory, so lets remove all
                # that are empty up till the base path.
                name = os.path.dirname(local_path)
                os.rmdir(name)
                head, tail = os.path.split(name)
                if not tail:
                    head, tail = os.path.split(head)
                while head and tail:
                    try:
                        logger.debug("[GCP][UPDATER] Removing dir %s.", head)
                        if head is base_path:
                            break
                        os.rmdir(head)
                    except OSError:
                        break
                    head, tail = os.path.split(head)
            except Exception:
                # The directory might not be empty, or maybe we don't have
                # permission. Either way doesn't really matter.
                pass

            media_deleted_files += 1
            deleted_files += 1
            deleted_bytes += size

        if media_uploaded_files:
            uploaded_media += 1

        if media_deleted_files:
            deleted_media += 1

        if media_deleted_files == len(local_files):
            # Mark as deleted only if *all* the local files have been deleted
            _mark_as_deleted(sqlite_conn, origin, media_id)

    logger.info("[GCP][UPDATER] Uploaded %s media out of %s", uploaded_media, total)
    logger.info("[GCP][UPDATER] Uploaded %s files", uploaded_files)
    logger.info("[GCP][UPDATER] Uploaded %s bytes", uploaded_bytes)
    logger.info("[GCP][UPDATER] Deleted %s media", deleted_media)
    logger.info("[GCP][UPDATER] Deleted %s files", deleted_files)
    logger.info("[GCP][UPDATER] Deleted %s bytes", deleted_bytes)

class GcpUpdaterModule(object):
    """Module that removes media folder files if they haven't been accessed in |duration| time."""
  
    def __init__(self, config: dict, api: ModuleApi):
        logger.debug("[GCP][UPDATER] Running GcpUpdaterModule __init__")
        
        self.cache_directory = api._hs.config.media.media_store_path
        self.reactor = api._hs.get_reactor()
        self.config = config
        self.key_path = config["key_path"]

        self._gcp_storage_pool = ThreadPool(
            name="gcp-updater-pool", maxthreads=config["threadpool_size"])
        self._gcp_storage_pool.start()

        # Manually stop the thread pool on shutdown. If we don't do this then
        # stopping Synapse takes an extra ~30s as Python waits for the threads
        # to exit.
        self.reactor.addSystemEventTrigger(
            "during", "shutdown", self._gcp_storage_pool.stop,
        )

        self._gcp_client = None
        self._gcp_client_lock = threading.Lock()
        
        # Setup parameters.
        self.cache_db: str = self.config["cache_db"]
        self.homeserver_db: str = self.config["homeserver_db"]
        self.duration: str = self.config["duration"]
        self.bucket: str = self.config["bucket"]

        def _loop():
            # with LoggingContext(parent_context=parent_logcontext):
            # while True:
            logger.info("[GCP][UPDATER] GcpUpdaterModule running loop in thread.")
            logger.debug("[GCP][UPDATER] duration: %s", self.duration)
            logger.debug("[GCP][UPDATER] cache_db: %s", self.cache_db)
            logger.debug("[GCP][UPDATER] cache_directory: %s", self.cache_directory)
            logger.debug("[GCP][UPDATER] homeserver_db: %s", self.homeserver_db)
            sqlite_conn = sqlite3.connect(self.cache_db)
            sqlite_conn.executescript(SCHEMA)
            synapse_db_conn = sqlite3.connect(self.homeserver_db)
            parsed_duration = _parse_duration(self.duration)
            _run_update_db(synapse_db_conn, sqlite_conn, parsed_duration)
            _run_check_delete(sqlite_conn, self.cache_directory)
            _run_upload(self._get_gcp_client(), self.bucket, sqlite_conn, self.cache_directory)

        def _error(failure: Failure):
            logger.error('[GCP][UPDATER] %s - %s: %s',
                str(failure.type).split("'")[1],
                failure.getBriefTraceback().split()[-1],
                failure.getErrorMessage(),
            )
        
        def _call_later():
            logger.debug("[GCP][UPDATER] GcpUpdaterModule running call later.")
            threads.deferToThreadPool(self.reactor, self._gcp_storage_pool, _loop).addErrback(_error)
            self.reactor.callLater(self.config["sleep_secs"], _call_later)
            
        _call_later()

    def _get_gcp_client(self) -> storage.Client:
        # this method is designed to be thread-safe, so that we can share a
        # single gcp client across multiple threads.
        #
        # (XXX: is creating a client actually a blocking operation, or could we do
        # this on the main thread, to simplify all this?)

        # first of all, do a fast lock-free check
        gcp_client = self._gcp_client
        if gcp_client:
            return gcp_client

        # no joy, grab the lock and repeat the check
        with self._gcp_client_lock:
            gcp_client = self._gcp_client
            if not gcp_client:
                gcp_client = storage.Client.from_service_account_json(self.key_path)
                self._gcp_client = gcp_client
            return gcp_client
        
    @staticmethod
    def parse_config(config: dict):
        rest_config: dict = {
            "bucket": config["bucket"],
            "key_path": config["key_path"],
            "duration": config.get("duration", "d10"),
            "threadpool_size": config.get("threadpool_size", 8),
            "cache_db": config.get("cache_db", "/data/cache.db"),
            "homeserver_db": config.get("homeserver_db", "/data/homeserver.db"),
            "sleep_secs": config.get("sleep_secs", 60 * 5),
        }
        return rest_config