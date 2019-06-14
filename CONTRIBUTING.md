# Contributing to Glutil

Thank you for being interested in contributing to Glutil!

## Building a test environment

_Note_: you'll need python 3.6 or later installed.

1.  Download this repo

    ``` bash
    git clone https://github.com/Journera/glutil.git
    ```

1.  Create a virtual environment inside the project, and activate it

    ``` bash
    cd glutil
    python3 -m venv env
    . env/bin/activate
    ```

1.  Install the development requirements

    ``` bash
    pip install -r requirements-dev.txt
    ```

1.  And now you're ready to get started!

## Testing

We strive for 100% test coverage, excluding only testing around AWS/Boto exception handling, because the way Boto manages exceptions is hard to fake.

A quick overview of our testing:

-   Tests are run using `nosetests`
-   Test coverage is generated using `coverage.py`, `nosetests` will print out a report of coverage after it's run.
-   `flake8` as a linter to keep the code style at least somewhat consistent.
-   The `sure` library is used to make tests more readable than plain asserts.
