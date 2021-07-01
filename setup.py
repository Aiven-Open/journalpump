from journalpump import __version__
from setuptools import find_packages, setup

import os

setup(
    name="journalpump",
    version=os.getenv("VERSION") or __version__,
    zip_safe=False,
    packages=find_packages(exclude=["test"]),
    extras_require={},
    install_requires=[
        "kafka-python",
        "requests",
        "websockets",
        "aiohttp-socks",
        "boto3",
        "google-api-python-client",
        "oauth2client",
        "geoip2",
        "typing-extensions",
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
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
)
