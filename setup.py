from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="DynamicAnnotationDB",
    version="0.1",
    author="Sven Dorkenwald",
    author_email="",
    description="Annotation Database pendant to the chunkedgraph",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/seung-lab/DynamicAnnotationDB",
    packages=find_packages(),
    install_requires=[
        "google-cloud-bigtable==0.28.1",
        "pytz"
    ],
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
    ),
)
