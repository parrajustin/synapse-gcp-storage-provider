from setuptools import setup

__version__ = "1.1"  # Bump to a 3-part semver for next version, i.e. 1.1.1

with open("README.md") as f:
    long_description = f.read()

setup(
    name="synapse-gcp-storage-provider",
    version=__version__,
    zip_safe=False,
    author="parrajustin",
    description="A storage provider which can fetch and store media in GCP storage.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/parrajustin/synapse-gcp-storage-provider",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
    ],
    py_modules=["gcp_storage_provider", "gcp_updater_module"],
    scripts=["scripts/gcp_media_upload"],
    install_requires=[
        "google-cloud-storage>=2.5.0",
        "humanize>=0.5.1<0.6",
        "psycopg2>=2.7.5<3.0",
        "PyYAML>=3.13<4.0",
        "tqdm>=4.26.0<5.0",
        "Twisted",
    ],
)
