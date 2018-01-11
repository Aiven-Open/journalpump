from journalpump import __version__
from setuptools import find_packages, setup
import os

setup(
    name="journalpump",
    version=os.getenv("VERSION") or __version__,
    zip_safe=False,
    packages=find_packages(exclude=["test"]),
    extras_require={},
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
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
    ],
)
