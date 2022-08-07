# -*- coding: utf-8 -*-
# Copyright 2018 New Vector Ltd
# Copyright 2021 The Matrix.org Foundation C.I.C.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import threading
from typing import Optional

from six import string_types
from dataclasses import dataclass
from google.cloud import storage


from twisted.internet.interfaces import IConsumer
from twisted.internet import defer, threads
from twisted.python.failure import Failure
from twisted.python.threadpool import ThreadPool

from synapse.types import ISynapseReactor
from synapse.logging.context import LoggingContext, make_deferred_yieldable
from synapse.rest.media.v1._base import Responder, FileInfo
from synapse.rest.media.v1.storage_provider import StorageProvider
from synapse.server import HomeServer
from google.cloud.storage.fileio import BlobReader


# @dataclass
# class Config:
#     bucket: str
#     key_path: str
#     threadpool_size: int


# Synapse 1.13.0 moved current_context to a module-level function.
try:
    from synapse.logging.context import current_context
except ImportError:
    current_context = LoggingContext.current_context

logger = logging.getLogger("synapse.gcp.storage_provider")

# Chunk size to use when reading from gcp connection in bytes
READ_CHUNK_SIZE = 16 * 1024


class _GcpResponder(Responder):
    """A Responder for gcp. Created by _GcpResponder
    """

    def __init__(self):
        # Triggered by responder when more data has been requested (or
        # stop_event has been triggered)
        self.wakeup_event = threading.Event()
        # Trigered by responder when we should abort the download.
        self.stop_event = threading.Event()

        # The consumer we're registered to
        self.consumer: Optional[IConsumer] = None

        # The deferred returned by write_to_consumer, which should resolve when
        # all the data has been written (or there has been a fatal error).
        self.deferred = defer.Deferred()

    def write_to_consumer(self, consumer: IConsumer):
        """See Responder.write_to_consumer
        """
        self.consumer = consumer
        # We are a IPushProducer, so we start producing immediately until we
        # get a pauseProducing or stopProducing
        consumer.registerProducer(self, True)
        self.wakeup_event.set()
        return make_deferred_yieldable(self.deferred)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop_event.set()
        self.wakeup_event.set()

    def resumeProducing(self):
        """See IPushProducer.resumeProducing
        """
        # The consumer is asking for more data, signal _GcpResponder
        self.wakeup_event.set()

    def pauseProducing(self):
        """See IPushProducer.stopProducing
        """
        self.wakeup_event.clear()

    def stopProducing(self):
        """See IPushProducer.stopProducing
        """
        # The consumer wants no more data ever, signal _GcpResponder
        self.stop_event.set()
        self.wakeup_event.set()
        if not self.deferred.called:
            with LoggingContext():
                self.deferred.errback(Exception("Consumer ask to stop producing"))

    def _write(self, chunk: bytes):
        """Writes the chunk of data to consumer. Called by _GcpResponder.
        """
        if self.consumer and not self.stop_event.is_set():
            self.consumer.write(chunk)

    def _error(self, failure):
        """Called when a fatal error occured while getting data. Called by
        _GcpResponder.
        """
        if self.consumer:
            self.consumer.unregisterProducer()
            self.consumer = None

        if not self.deferred.called:
            self.deferred.errback(failure)

    def _finish(self):
        """Called when there is no more data to write. Called by _GcpResponder.
        """
        if self.consumer:
            self.consumer.unregisterProducer()
            self.consumer = None

        if not self.deferred.called:
            self.deferred.callback(None)


class GcpStorageProviderBackend(StorageProvider):
    """
    Args:
        hs (HomeServer)
        config: The config returned by `parse_config`
    """

    def __init__(self, hs: HomeServer, config: dict):
        self.cache_directory = hs.config.media.media_store_path
        self.bucket = config["bucket"]
        self.key_path = config["key_path"]

        self.reactor = hs.get_reactor()

        self._gcp_client = None
        self._gcp_client_lock = threading.Lock()

        threadpool_size = config.get("threadpool_size", 40)
        self._gcp_storage_pool = ThreadPool(
            name="gcp-storage-pool", maxthreads=threadpool_size)
        self._gcp_storage_pool.start()

        # Manually stop the thread pool on shutdown. If we don't do this then
        # stopping Synapse takes an extra ~30s as Python waits for the threads
        # to exit.
        self.reactor.addSystemEventTrigger(
            "during", "shutdown", self._gcp_storage_pool.stop,
        )

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

    def store_file(self, path: str, file_info: FileInfo) -> None:
        """See StorageProvider.store_file

        Args:
            path: Relative path of file in local cache
            file_info: The metadata of the file.
        """

        parent_logcontext = current_context()

        def _store_file():
            with LoggingContext(parent_context=parent_logcontext):
                client = self._get_gcp_client()
                bucket = client.bucket(self.bucket)
                blob = bucket.blob(path)
                blob.upload_from_filename(path)
                logger.debug("[GCP] Storing file \"%s\".".format(path))

        threads.deferToThreadPool(self.reactor, self._gcp_storage_pool, _store_file)

    def fetch(self, path: str, file_info: FileInfo) -> Optional[Responder]:
        """See StorageProvider.fetch

        Args:
            path: Relative path of file in local cache
            file_info: The metadata of the file.
        """
        logcontext = current_context()

        d = defer.Deferred()

        def _get_file():
            gcp_download_task(self._get_gcp_client(), self.bucket, path, self.reactor, d, logcontext)

        self._gcp_storage_pool.callInThread(_get_file)
        return make_deferred_yieldable(d)

    @staticmethod
    def parse_config(config: any) -> Config:
        """Called on startup to parse config supplied. This should parse
        the config and raise if there is a problem.

        The returned value is passed into the constructor.

        In this case we return a dict with fields, `bucket` and `storage_class`
        """
        result = {
            "bucket": config["bucket"],
            "key_path": config["key_path"],
            "threadpool_size": config["threadpool_size"]
        }

        return result


def gcp_download_task(gcp_client: storage.Client, bucket: str, key: str, reactor: ISynapseReactor, deferred: defer.Deferred, parent_logcontext):
    """Attempts to download a file from gcp.

    Args:
        gcp_client: gcp storage client
        bucket (str): The gcp storage bucket which may have the file
        key (str): The key of the file
        deferred (Deferred[_GcpResponder|None]): If file exists
            resolved with an _GcpResponder instance, if it doesn't
            exist then resolves with None.
        parent_logcontext (LoggingContext): the logcontext to report logs and metrics
            against.
    """
    with LoggingContext(parent_context=parent_logcontext):
        logger.info("Fetching %s from gcp", key)

        gcp_bucket = gcp_client.bucket(bucket)
        blob = gcp_bucket.blob(key)

        if not blob.exists():
            logger.error("Media \"%s\" not found in gcp.", key)
            reactor.callFromThread(deferred.callback, None)
            return

        data: BlobReader = None
        try:
            data = blob.open(mode="rb", chunk_size=READ_CHUNK_SIZE)
        except Exception as e:
            logger.error("Error key \"%s\" downloading from gcp.", key)

            if e.response["Error"]["Code"] in ("404", "NoSuchKey",):
                logger.info("Media %s not found in S3", key)
                reactor.callFromThread(deferred.callback, None)
                return

            reactor.callFromThread(deferred.errback, Failure())
            return

        producer = _GcpResponder()
        reactor.callFromThread(deferred.callback, producer)
        _stream_to_producer(reactor, producer, data, timeout=90.0)


def _stream_to_producer(reactor: ISynapseReactor, producer: _GcpResponder, data: BlobReader, status=None, timeout=None):
    """Streams a file like object to the producer.

    Correctly handles producer being paused/resumed/stopped.

    Args:
        reactor
        producer (_GcpResponder): Producer object to stream results to
        body (file like): The object to read from
        status (_ProducerStatus|None): Used to track whether we're currently
            paused or not. Used for testing
        timeout (float|None): Timeout in seconds to wait for consume to resume
            after being paused
    """

    # Set when we should be producing, cleared when we are paused
    wakeup_event = producer.wakeup_event

    # Set if we should stop producing forever
    stop_event = producer.stop_event

    if not status:
        status = _ProducerStatus()

    try:
        while not stop_event.is_set():
            # We wait for the producer to signal that the consumer wants
            # more data (or we should abort)
            if not wakeup_event.is_set():
                status.set_paused(True)
                ret = wakeup_event.wait(timeout)
                if not ret:
                    raise Exception("Timed out waiting to resume")
                status.set_paused(False)

            # Check if we were woken up so that we abort the download
            if stop_event.is_set():
                return

            chunk = data.read(READ_CHUNK_SIZE)
            if not chunk:
                return

            reactor.callFromThread(producer._write, chunk)

    except Exception:
        reactor.callFromThread(producer._error, Failure())
    finally:
        reactor.callFromThread(producer._finish)
        if data:
            data.close()


class _ProducerStatus(object):
    """Used to track whether the gcp download thread is currently paused
    waiting for consumer to resume. Used for testing.
    """

    def __init__(self):
        self.is_paused = threading.Event()
        self.is_paused.clear()

    def wait_until_paused(self, timeout=None):
        is_paused = self.is_paused.wait(timeout)
        if not is_paused:
            raise Exception("Timed out waiting")

    def set_paused(self, paused):
        if paused:
            self.is_paused.set()
        else:
            self.is_paused.clear()
