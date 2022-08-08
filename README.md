Synapse GCP Storage Provider
===========================

This module can be used by synapse as a storage provider, allowing it to fetch
and store media in Google Cloud Storage.


Usage
-----

Example of entry in synapse config:

```yaml
modules:
- module: auth_rest_provider.RestAuthProvider
  config:
    # Auth rest endpoint.
    endpoint: "some_ip"
- module: gcp_updater_module.GcpUpdaterModule
  config:
    # GCP bucket.
    bucket: "synapse_data_store"
    # Duration afterwhich the file is removed, accepts the following suffix (s, m, h, d, M, y). Example 1M for 1 month.
    duration: "1d"
    # (optional) The threadpool size currently not used...
    threadpool_size: 8
    # GCP storage service account json.
    key_path: "/data/gcsfuse-key.json"
    # The cache db to use. Should be in memory since GcpStorageProviderBackend downloads files and it doesn't this module.
    cache_db: ":memory:"
    # The homeserver link, only supports sqlite3 since I removed the postgress code.
    homeserver_db: "/data/homeserver.db"
    # Seconds to sleep the updater thread.
    sleep_secs: 600

media_storage_providers:
- module: gcp_storage_provider.GcpStorageProviderBackend
  store_local: True
  store_remote: True
  store_synchronous: True
  config:
    # GCP bucket.
    bucket: "synapse_data_store"
    # GCP storage service account json.
    key_path: "/data/gcsfuse-key.json"
    # Max number of threads.
    threadpool_size: 40
```


Regular cleanup job
-------------------

Regular clean up is moved to an updater module that runs automatically.

Packaging and release
---------

For maintainers:

1. Update the `__version__` in setup.py. Commit. Push.
2. Create a release on GitHub for this version.



## Installing virtualenv

```bash
pip install virtualenv
virtualenv synapse-gcp-storage-provider
source synapse-gcp-storage-provider/bin/activate
synapse-gcp-storage-provider/bin/pip install google-cloud-storage
```