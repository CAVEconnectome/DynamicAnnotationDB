from setuptools import setup, find_packages
import os
import codecs
import re

here = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    with codecs.open(os.path.join(here, *parts), 'r') as fp:
        return fp.read()

def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")

with open("README.md", "r") as fh:
    long_description = fh.read()

with open('requirements.txt', 'r') as f:
    required = f.read().splitlines()

setup(
    name="DynamicAnnotationDB",
    version=find_version("dynamicannotationdb", "__init__.py"),
    author="Sven Dorkenwald, Derrick Brittain",
    author_email="sdorkenw@princeton.edu",
    description="Annotation Database pendant to the chunkedgraph",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/seung-lab/DynamicAnnotationDB",
    packages=find_packages(),
    install_requires=required,
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
    ),
)
