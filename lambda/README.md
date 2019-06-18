# Replacement Glue Crawler Lambda

One of our primary uses of `glutil` is as a Glue Crawler analog for tables and data that the Glue Crawler cannot crawl.
Specifically, we've run into issues running crawlers over data in non Hive-partitioned path schemas.
To overcome this, we created a lambda that operates similarly for crawling S3 data.

As with the rest of `glutil` this lambda makes the same assumptions noted in the [main readme](../README.md#built-in-assumptions).

## Building a Lambda-Compatible zip

The code for running the lambda is included in the `glutil` library, but just uploading the library and configuring the lambda to use a function inside the module runs into problems.
To overcome this, we provide a `build-lambda-zip.sh` script, which will pull in the `glutil` module and add [`lambda.py`](./lambda.py) to call into the module.

To build a lambda zip, run `./build-lambda-zip.sh`.
It will create a `lambda.zip` file, which you can upload to AWS Lambda.
The handler to use is `lambda.handler`

## Managing the Lambda with Terraform

If you use terraform, you can use the provided [`terraform-example.tf`](./terraform-example.tf) as a starting point for deploying the lambda.
You will most likely need to make some minor changes, but for the most part it can be used as-is.
