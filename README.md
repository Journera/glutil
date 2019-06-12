# Glutil

A collection of utilities for managing AWS Athena/Glue partitions.

## Background

Amazon's Athena and Glue are generally pretty great, but sometimes things don't play well together, or a configuration mistake was made.
For those cases, we have these utilities.

At Journera, our original use case for this project was as a Glue Crawler replacement for tables that don't use Hive-compliant path structures.
For the most part this is a workaround, because at present Terraform (which we use to manage our Firehoses) does not support using formatted prefixes with data written in JSON.

## Provided Utilities

There are three main ways to use these utilities, either by using the `glutil` library in your python code, by using the provided `glutil` command line script, or as a lambda replacement for a Glue Crawler.

## Built-in Assumptions

Because `glutil` started life as a way to work with Journera-managed data there are still a number of assumptions built in to the code.
Ideally these will be removed in the future to enable use with more diverse sets of data.

1.  The tables use S3 as their backing data store.

1.  All partitions are stored under the table's location.

    For example, if you have a table with the location `s3://some-data-bucket/my-table/`, `glutil` will only find partitions located in `s3://some-data-bucket/my-table/`.
    You table location can be as deep or shallow as you want, `glutil` will operate the same for a table located in `s3://bucket/path/to/table/it/goes/here/` and `s3://bucket/`.

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

## `glutil` command line interface

The `glutil` CLI includes a number of subcommands for managing partitions and fixing a glue catalog when things go wrong.
Most of the commands were written to fix issues caused by a Glue Crawler gone wrong, moving underlying data, or dealing with newly created data.

For the most part, they operate with the leading principle that any action they take can be reversed (if it was an incorrect action) by running the `glutil create-partitions` command.

All commands support the `--dry-run` flag, which will output the command's expected result without modifying the glue catalog.

Below are short descriptions of the available commands.
For larger descriptions and command line arguments, run `glutil <command> --help`.

### `glutil create-partitions`

`create-partitions` is the original use case for this code.
Running it will search S3 for partitioned data, and will create new partitions for data missing from the glue catalog.

### `glutil delete-all-partitions`

`delete-all-partitions` will query the glue catalog and delete any partitions attached
to the specified table.
For the most part it is substantially faster to just delete the entire table and recreate it because of AWS batch limits, but sometimes it's harder to recreate than to remove all partitions.

### `glutil delete-bad-partitions`

`delete-bad-partitions` will remove partitions that meet the following criteria from the catalog:

- Partitions without any data in S3
- Partitions with values that do not match their S3 location (ex. Partition with values `[2019 01 02 03]` with a location of anything other than `s3://table/path/2019/01/02/03/`)

In general, if you use `glutil create-partition` multiple times and see the same partition attempt to be created both times, you should run `delete-bad-partitions` and try `create-partitions` again.

### `glutil delete-missing-partitions`

`delete-missing-partitions` will remove any partition in the glue catalog without data in S3.

### `glutil update-partitions`

`update-partitions` should be run after moving your data in S3 and updating your table's location in the catalog.
It updates partitions by finding all partitions in S3, and checking if a partition with matching values exists in the catalog.
If it finds a matching partition, it updates the existing partition with the new location.

### `glutil delete-bad-tables`

Sometimes when running a glue crawler, the crawler doesn't your table's data correctly, and instead sees what should be partitions as tables.
When this happens, it may create a large number of junk tables in the catalog.
`delete-bad-tables` should be run to fix this.

`delete-bad-tables` deletes any tables in your glue catalog that meet the following criteria:

-   A table with a path that is below another table's path.

    For example, if you have two tables, with these paths:

    ```
    s3://some-data-bucket/table-path/, and
    s3://some-data-bucket/table-path/another-table/
    ```

    The table at `s3://some-data-bucket/table-path/another-table/` will be deleted.

-   A table with the same location as another, with a name that's a superstring of the other's (this is from the glue crawler semantic of creating tables which would otherwise have the same name with the name {table}-somelongid).

    For example, if you have the tables `foo` and `foo-buzzer`, both with the same location, `foo-buzzer` will be deleted.

## Running `create-partitions` as a Lambda

Journera's biggest use for this library is as a Glue Crawler replacement for tables and datasets the glue crawlers have problems parsing.
Information on this lambda can be found in the [lambda](./lambda) directory.
