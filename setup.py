from setuptools import setup, find_packages
from kafkajournalpump import __version__
import os

setup(
    name="kafkajournalpump",
    version=os.getenv("VERSION") or __version__,
    zip_safe=False,
    packages=find_packages(exclude=["test"]),
    install_requires=["kafka-python >= 0.9.0"],
    extras_require={},
    dependency_links=[],
    package_data={},
    data_files=[],
    entry_points={'console_scripts': ["kafkajournalpump = kafkajournalpump.__main__:main"]}
)
