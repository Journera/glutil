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

resource "aws_s3_bucket" "bucket" {
  name = "${var.table_storage_bucket}"
}

resource "aws_glue_catalog_database" "database" {
  name = "${var.glue_database_name}"
}

resource "aws_glue_catalog_table" "table" {
  name          = "${var.glue_table_name}"
  database_name = "${var.glue_database_name}"

  table_type = "EXTERNAL_TABLE"

  parameters {
    EXTERNAL = "TRUE"
  }

  partition_keys = [
    {
      name = "year"
      type = "int"
    },
    {
      name = "month"
      type = "int"
    },
    {
      name = "day"
      type = "int"
    },
    {
      name = "hour"
      type = "int"
    },
  ]

  storage_descriptor {
    location      = "s3://${var.table_storage_bucket}/${var.glue_table_name}/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    compressed    = false

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"

      parameters {
        ignore.malformed.json = "true"
        serialization.format  = 1
      }
    }

    columns = [{
      name = "field"
      type = "string"
    }]
  }
}

resource "aws_iam_role" "lambda-iam" {
  name   = "lambda-role"
  policy = "${data.aws_iam_policy_document.lambda-iam.json}"
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

resource "aws_iam_role_policy" "partitioner-permisions" {
  role   = "${aws_iam_role.lambda-iam.name}"
  name   = "Partitioner Permissions"
  policy = "${data.aws_iam_policy_document.partitioner-permissions.json}"
}

data "aws_iam_policy_document" "partitioner-permissions" {
  statement {
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:BatchCreatePartition",
      "glue:GetPartitions",
    ]

    resources = ["*"]
  }

  statement {
    actions = [
      "s3:List*",
      "s3:GetObject",
      "s3:PutPbject",
    ]

    resources = [
      "${aws_s3_bucket.bucket.arn}",
      "${aws_s3_bucket.bucket.arn}/*",
    ]
  }
}

# NOTE: you should explicitly change this
data "archive_file" "glutil-code" {
  type        = "zip"
  source_dir  = "${path.module}/../../"
  output_path = "${path.module}/glutil.zip"
}

resource "aws_lambda_function" "glue-partitioner" {
  function_name = "glue-partitioner"
  role          = "${aws_iam_role.lambda-iam.arn}"
  handler       = "athena_partitioner.handle"

  filename         = "${path.module}/glutil.zip"
  source_code_hash = "${filebase64sha256("${path.module}/glutil.zip")}"

  runtime = "python3.6"
  timeout = 30
}

resource "aws_cloudwatch_event_rule" "partitioner-timer" {
  name                = "glue-partitioner-trigger"
  description         = "Trigger the Glue partitioner every hour"
  schedule_expression = "cron(08 0/1 * * ? *)"
}

resource "aws_cloudwatch_event_target" "partitioner-timer" {
  rule  = "${aws_cloudwatch_event_rule.partitioner-timer.name}"
  arn   = "${aws_lambda_function.glue-partitioner.arn}"
  input = "{\"database\": \"${var.glue_database_name}\", \"table\": \"${var.glue_table_name}\"}"
}
