from journalpump import __version__
from setuptools import find_packages, setup

import os

setup(
    name="journalpump",
    version=os.getenv("VERSION") or __version__,
    zip_safe=False,
    packages=find_packages(exclude=["test", "systest"]),
    extras_require={},
    install_requires=[
        "kafka-python<2.0.4",
        "requests",
        "websockets",
        "aiohttp-socks",
        "botocore",
        "google-api-python-client",
        "google-auth",
        "geoip2",
    ],
    dependency_links=[],
    package_data={},
    data_files=[],
    entry_points={
        "console_scripts": [
            "journalpump = journalpump.__main__:main",
        ],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: System :: Logging",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
