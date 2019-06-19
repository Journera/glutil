# Glutil

A collection of utilities for managing partitions of tables in the AWS Glue Data Catalog that are built on datasets stored in S3.

## Background

AWS's Glue Data Catalog provides an index of the location and schema of your data across AWS data stores and is used to reference sources and targets for ETL jobs in AWS Glue. It is fully-integrated with AWS Athena, an ad-hoc query tool that uses the Hive metastore to build external tables on top of S3 data and PrestoDB to query the data with standard SQL.

Journera heavily uses Kinesis Firehoses to write data from our platform to S3 in near real-time, Athena for ad-hoc analysis of data on S3, and Glue's serverless engine to execute PySpark ETL jobs on S3 data using the tables defined in the Data Catalog.
Using the Data Catalog is generally pretty great, but sometimes all of these managed services don't play well together, or a configuration mistake was made (e.g., in a table DDL).
For those cases, we have these utilities.

Our original use case for this project was as a Glue Crawler replacement for adding new partitions to tables that don't use Hive-style partitions and for tables built on top of S3 datasets that the Glue Crawler could not successfully parse.
For the most part this is a workaround, because of current limitations with the Glue Crawler and Terraform, which does not support configuring Kinesis Firehoses to write JSON data to S3 using formatted prefixes.

## Installation

`glutil` can be installed using pip.

``` bash
pip install glutil
```

If you wish to manually install it, you can clone the repository and run

``` bash
python3 setup.py install
```

## Provided Utilities

There are three main ways to use these utilities, either by using the `glutil` library in your python code, by using the provided `glutil` command line script, or as a lambda replacement for a Glue Crawler.

## Built-In Assumptions

Because `glutil` started life as a way to work with Journera-managed data there are still a number of assumptions built in to the code.
Ideally these will be removed in the future to enable use with more diverse sets of data.

1.  The tables use S3 as their backing data store.

1.  All partitions are stored under the table's location.

    For example, if you have a table with the location `s3://some-data-bucket/my-table/`, `glutil` will only find partitions located in `s3://some-data-bucket/my-table/`.
    Your table location can be as deep or shallow as you want, `glutil` will operate the same for a table located in `s3://bucket/path/to/table/it/goes/here/` and `s3://bucket/`.

1.  Your partition keys are `[year, month, day, hour]`.

1.  Your partitions are stored in one of these two path schemas, assuming your table is located at `s3://bucket/table/`:

    ```
    s3://bucket/table/YYYY/MM/DD/HH/
    s3://bucket/table/year=YYYY/month=MM/day=DD/hour=HH/
    ```

## IAM Permissions

To use `glutil` you need the following IAM permissions:

- `glue:GetDatabase`
- `glue:GetTable`
- `glue:GetTables`
- `glue:BatchCreatePartition`
- `glue:BatchDeleteTable`
- `glue:BatchDeletePartition`
- `glue:GetPartitions`
- `glue:UpdatePartition`
- `s3:ListBucket` on the buckets containing your data
- `s3:GetObject` on the buckets containing your data

If you're only using the `create-partition` lambda, you can get by with only:

- `glue:GetDatabase`
- `glue:GetTable`
- `glue:BatchCreatePartition`
- `glue:GetPartitions`
- `s3:ListBucket`
- `s3:GetObject`

## `glutil` Command Line Interface (CLI)

The `glutil` CLI includes a number of subcommands for managing partitions and fixing the Glue Data Catalog when things go wrong.
Most of the commands were written to fix issues caused by a Glue Crawler gone wrong, moving underlying data, or dealing with newly created data.

For the most part, they operate with the leading principle that any action they take can be reversed (if it was an incorrect action) by running the `glutil create-partitions` command.

All commands support the `--dry-run` flag, which will output the command's expected result without modifying the Glue Data Catalog.

Below are short descriptions of the available commands.
For larger descriptions and command line arguments, run `glutil <command> --help`.

### `glutil create-partitions`

`create-partitions` is the original use case for this code.
Running it will search S3 for partitioned data, and will create new partitions for data missing from the Glue Data Catalog.

### `glutil delete-all-partitions`

`delete-all-partitions` will query the Glue Data Catalog and delete any partitions attached
to the specified table.
For the most part it is substantially faster to just delete the entire table and recreate it because of AWS batch limits, but sometimes it's harder to recreate than to remove all partitions.

### `glutil delete-bad-partitions`

`delete-bad-partitions` will remove partitions that meet the following criteria from the catalog:

- Partitions without any data in S3
- Partitions with values that do not match their S3 location (ex. Partition with values `[2019 01 02 03]` with a location of anything other than `s3://table/path/2019/01/02/03/`)

In general, if you use `glutil create-partitions` multiple times and see attempts to create the same partition both times, you should run `delete-bad-partitions` and try `create-partitions` again.

### `glutil delete-missing-partitions`

`delete-missing-partitions` will remove any partition in the Glue Data Catalog without data in S3.

### `glutil update-partitions`

`update-partitions` should be run after moving your data in S3 and updating your table's location in the catalog.
It updates partitions by finding all partitions in S3, and checking if a partition with matching values exists in the catalog.
If it finds a matching partition, it updates the existing partition with the new location.

### `glutil delete-bad-tables`

Sometimes when running a Glue Crawler, the crawler doesn't aggregate the data correctly, and instead creates tables for individual partitions.
When this happens, it may create a large number of junk tables in the catalog.
`delete-bad-tables` should be run to fix this.

`delete-bad-tables` deletes any tables in your Glue Data Catalog that meet the following criteria:

-   A table with a path that is below another table's path.

    For example, if you have two tables with these paths:

    ```
    s3://some-data-bucket/table-path/, and
    s3://some-data-bucket/table-path/another-table/
    ```

    The table at `s3://some-data-bucket/table-path/another-table/` will be deleted.

-   A table with the same location as another, with a name that's a superstring of the other's (this is from the Glue Crawler semantic of creating tables which would otherwise have the same name with the name {table}-somelongid).

    For example, if you have the tables `foo` and `foo-buzzer`, both with the same location, `foo-buzzer` will be deleted.

## Running `create-partitions` as a Lambda

Journera's biggest use for this library is as a Glue Crawler replacement for tables and datasets the Glue Crawlers have problems parsing.
Information on this lambda can be found in the [lambda](./lambda) directory.

## Contributing to Glutil
This project was recently open-sourced. As such, please pardon any sharp edges, and let us know about them by [creating an issue](https://github.com/Journera/glutil/issues/new).

All contributions, bug reports, bug fixes, documentation improvements, enhancements and ideas are welcome.

A detailed overview on how to contribute can be found in the [contributing guide](CONTRIBUTING.md).

## License
[BSD 3](LICENSE)
