# Glue Partitioner using Glutils

[`main.tf`](./main.tf) has the basic outline of what's needed to run the partitioner lambda.

The terraform file assume you have nothing setup already, and creates:

- The Glue database
- The Glue table for the partition to run on
- The S3 bucket where the partitioned data lives

If you already have those, you can adjust the terraform as needed.

The terraform file also assumes you'll want to store the table data in `s3://table_storage_bucket/glue_table_name/`.
