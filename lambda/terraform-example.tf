variable "glue_database_name" {
  type        = "string"
  default     = "example_database"
  description = "Name of the Glue database to create and run the lambda off"
}

variable "glue_table_name" {
  type        = "string"
  default     = "example_table"
  description = "Name of the Glue table to create and run the lambda off"
}

variable "table_storage_bucket" {
  type        = "string"
  default     = "example_bucket"
  description = "Name of the S3 bucket where the data for the Glue table is stored"
}

variable "lambda_file" {
  type        = "string"
  default     = "lambda.zip"
  description = "The path of the zipped folder to be used as the lambda source, relative to this file."
}

# create a reference to the full lambda path, for use later
locals {
  lambda_file = "${path.module}/${var.lambda_file}"
}

# Find your table storage bucket, it'll be used later to give the lambda
# permissions to crawl it for new data.
data "aws_s3_bucket" "bucket" {
  bucket = "${var.table_storage_bucket}"
}

# Setup an IAM Role for the lambda to assume.
resource "aws_iam_role" "lambda-iam" {
  name               = "partitioner_lambda"
  assume_role_policy = "${data.aws_iam_policy_document.lambda-iam.json}"
}

data "aws_iam_policy_document" "lambda-iam" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals = [{
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }]
  }
}

# Give the new IAM role permissions to modify the glue catalog and access your
# table's S3 data.
resource "aws_iam_role_policy" "partitioner-permisions" {
  role   = "${aws_iam_role.lambda-iam.name}"
  name   = "partitioner_permissions"
  policy = "${data.aws_iam_policy_document.partitioner-permissions.json}"
}

data "aws_iam_policy_document" "partitioner-permissions" {
  # Allow glue catalog access
  statement {
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:BatchCreatePartition",
      "glue:GetPartitions",
    ]

    resources = ["*"]
  }

  # Allow S3 access
  statement {
    actions = [
      "s3:List*",
      "s3:GetObject",
    ]

    resources = [
      "${data.aws_s3_bucket.bucket.arn}",
      "${data.aws_s3_bucket.bucket.arn}/*",
    ]
  }
}

# Create the actual lambda function
resource "aws_lambda_function" "glue-partitioner" {
  function_name = "glue-partitioner"
  role          = "${aws_iam_role.lambda-iam.arn}"
  handler       = "lambda.handler"

  filename = "${local.lambda_file}"

  source_code_hash = "${base64sha256(file("${local.lambda_file}"))}"

  runtime = "python3.6"
  timeout = 30
}

# Setup a cloudwatch event to trigger every hour on the 8th minute
resource "aws_cloudwatch_event_rule" "partitioner-timer" {
  name                = "glue-partitioner-trigger"
  description         = "Trigger the Glue partitioner every hour"
  schedule_expression = "cron(08 0/1 * * ? *)"
}

# Run the lambda when the cloudwatch event is fired
resource "aws_cloudwatch_event_target" "partitioner-timer" {
  rule  = "${aws_cloudwatch_event_rule.partitioner-timer.name}"
  arn   = "${aws_lambda_function.glue-partitioner.arn}"
  input = "{\"database\": \"${var.glue_database_name}\", \"table\": \"${var.glue_table_name}\"}"
}

# Give the event permission to invoke the lambda
resource "aws_lambda_permission" "allow-partition-timer-to-invoke-lambda" {
  statement_id  = "allow-timer-for-partitioner"
  action        = "lambda:InvokeFunction"
  function_name = "${aws_lambda_function.glue-partitioner.function_name}"
  source_arn    = "${aws_cloudwatch_event_rule.partitioner-timer.arn}"
  principal     = "events.amazonaws.com"
}
