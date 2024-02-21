"""
A Python library for interacting with the validityBase (vBase) platform
"""

from setuptools import find_packages, setup

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

with open("requirements.txt", encoding="utf-8") as f:
    requirements = f.read().splitlines()

setup(
    name="vbase",
    version="0.0.1",
    author="PIT Labs Inc.",
    author_email="tech@pitlabs.xyz",
    description="A Python library for interacting with the validityBase (vBase) platform",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pit-labs/vbase-py",
    packages=find_packages(),
    package_data={
        "": ["../requirements.txt", "abi/*.json"],
    },
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
