import os
import sys

from setuptools import setup, find_packages


setup(
    name="python-arango",
    description="Python Driver for ArangoDB",
    version="2.0.0",
    author="Joohwan Oh",
    author_email="joohwan.oh@outlook.com",
    url="https://github.com/Joowani/python-arango",
<<<<<<< Updated upstream
    download_url="https://github.com/Joowani/python-arango/tarball/1.4.0",
=======
    download_url="https://github.com/Joowani/python-arango/tarball/1.0.0",
>>>>>>> Stashed changes
    packages=find_packages(),
    include_package_data=True,
    install_requires=["requests"],
    test_suite="nose",
)
