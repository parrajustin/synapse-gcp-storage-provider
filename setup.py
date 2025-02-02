from setuptools import setup

__version__ = "1.0.0"

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
    py_modules=["gcp_storage_provider", "gcp_updater_module", "auth_rest_provider"],
    scripts=[],
    install_requires=[
        "google-cloud-storage>=2.5.0",
        "Twisted",
    ],
)
