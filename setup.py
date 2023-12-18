import os

from setuptools import setup

setup(
    name="bubbaloo",
    version="0.0.10",
    description="This package is designed to make it easy to build data pipelines using pyspark",
    url="https://github.com/4512ronald/bubaloo",
    author="Ronald Restrepo",
    author_email="rarestrepoc@gmail.com",
    license="Apache Software License",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 1 - Planning"
    ],
    keywords=["google-cloud-storage", "pyspark", "data pipelines"],
    # install_requires=[open("requirements.txt").read().strip().split("\n")],
    long_description=(
        open("README.md").read() if os.path.exists("README.md") else ""
    ),
    python_requires=">=3.10.12",
    zip_safe=False,
    project_urls={
        "Homepage": "https://github.com/4512ronald/bubaloo",
        "Bug Tracker": "https://github.com/4512ronald/bubaloo/issues"
    },
    setup_requires=["setuptools>=61.0"],
    include_package_data=True
)
