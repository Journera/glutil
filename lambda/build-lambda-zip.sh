#!/usr/bin/env bash

this_dir=$PWD
lambda_zip=${this_dir}/lambda.zip

# jump up to project root directory
pushd ../ > /dev/null
  # add glutil library to zip
  zip -rq ${lambda_zip} glutil/
popd > /dev/null

# add the lambda.py file
zip -q ${lambda_zip} lambda.py

# remove all pyc and __pycache__ files
zip -dq ${lambda_zip} '*.pyc' '*/__pycache__/*'
