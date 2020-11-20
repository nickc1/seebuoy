# Contributing

## Development Install

First clone the repo and change into the directory:

```
git clone https://github.com/nickc1/seebuoy.git
cd seebuoy
```

To install for local development, first install [poetry](https://python-poetry.org/). Then all you need to run is:

```
poetry install
```

and all dependencies (including development dependencies) will be installed.

To run the tests, first activate the virtual environment:

```
poetry shell
```

Then run pytest:

```
pytest
```

## Creating a Release

We follow the same release process that poetry itself uses. You can read about it [here](https://python-poetry.org/docs/contributing/#git-workflow).

To quote poetry's docs:

When a release is ready, the following are required before a release is tagged.

1. A release branch with the prefix release-, eg: release-1.1.0rc1.
2. A pull request from the release branch to the main branch (master) if it's a minor or major release. Otherwise, to the bug fix branch (eg: 1.0).
3. The pull request description MUST include the change log corresponding to the release (eg: #2971).
4. The pull request must contain a commit that updates CHANGELOG.md and bumps the project version (eg: #2971).
5. The pull request must have the Release label specified.
