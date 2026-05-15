# Contributing to the vBase Python Software Development Kit (SDK)

Thank you for considering contributing to the vBase Python Software Development Kit (SDK)!

# How to Contribute

## Reporting Issues

If you encounter a bug or have a feature request,
please use the GitHub Issues section of the repository to report it:
- Check Existing Issues:
Make sure the issue has not already been reported or addressed.
- Create a New Issue:
If your issue is new, click the "New Issue" button and select the appropriate template
(bug report or feature request).
- Fill Out the Template:
Provide as much information as possible to help us understand the problem or feature.

## Making Contributions

To contribute code or documentation, please do the following:
- Create a fork of the repository.
- Create a new branch with your change, and push the changes to it.
- Submit a pull request for your change.
Provide a detailed description of the changes and any supporting information.

## Publishing Releases

- Update the package version in `vbase/_version.py`.
- For a test publish, run the `Publish Python Package to PyPI` workflow manually and choose `testpypi`.
- For a production publish, create and publish a GitHub Release or run the same workflow manually and choose `pypi`.
- Configure GitHub environments named `testpypi` and `pypi` and add matching trusted publishers in TestPyPI and PyPI before the first publish.
