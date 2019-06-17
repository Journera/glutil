#!/usr/bin/env bash

set -x -e
flake8

rm -f .coverage
rm -rf cover/

nosetests -v --nologcapture --with-coverage --cover-html ./tests/
