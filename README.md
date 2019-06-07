# Athena/Glue Partition Management Utilities

Amazon's Athena and Glue are generally pretty great, but sometimes things don't play well together, or a configuration mistake was made.
For those cases, we have these utilities.

At Journera, our main use case for this project is enabling partitions on tables written by Kinesis Firehoses which are not writing data in Hive-compliant directory structures.
For the most part this is a workaround, because at present Terraform (which we use to manage our Firehoses) does not support using formatted prefixes with data written in JSON.

## Setup

We develop and run these scripts with Python 3.x.
Our only dependency is boto3, for communicating with AWS.

## `athena_partitioner.py`

`athena_partitioner` is meant to be an alternative to AWS Glue Crawlers for Athena/Glue tables that are backed by S3.

The Glue Crawlers work great, if you have your data stored in Hive-compliant partition directory structures.
Unfortunately that's not always possible, so we've created this lambda.

At present, the `athena_partitioner` can crawl S3-backed tables where data is kept in the following structures:

```
s3://bucket/any/path/to/table/YYYY/MM/DD/HH/data_file.ext
s3://bucket/any/path/to/table/year=YYYY/month=MM/day=DD/hour=HH/data_file.ext
```

The partitioner can be run as a lambda to pick up new data hourly for frequently used tables, or locally for tables only occasionally used.
For more information, see [Running the Partitioner](#running-the-partitioner).


### Semantics

The way the partitioner works is by crawling S3, starting from a glue table's location, and searching for `year/month/day/hour/` directory structures.
For all the structures it finds, it determines if a partition matching the found values exist, and if not, creating the found partitions.

For example, assume you have a Glue table called stored in `s3://some-data-bucket/glue/my-table/`, and you have the following data in S3 that should be added as partitions to the table:

```
s3://some-data-bucket/glue/my-table/2019/01/02/03/data-1.json
s3://some-data-bucket/glue/my-table/2019/01/02/22/data-2.json
s3://some-data-bucket/glue/my-table/2019/03/13/15/data-3.json
```

The partitioner will find the following partitions:

```
year=2019 month=01 day=02 hour=03
year=2019 month=01 day=02 hour=22
year=2019 month=03 day=13 hour=15
```

From there, the partitioner will determine, for each of those, if the partition already exists, and will create a new partition for each one that doesn't.

Similarly, the partitioner will also work on data in this structure (and result in the same partitions as above):

```
s3://some-data-bucket/glue/my-table/year=2019/month=01/day=02/hour=03/data-1.json
s3://some-data-bucket/glue/my-table/year=2019/month=01/day=02/hour=22/data-2.json
s3://some-data-bucket/glue/my-table/year=2019/month=03/day=13/hour=15/data-3.json
```

### Running the partitioner

`athena_partitioner.py` can be invoked as a lambda or locally.

Invoking it as a lambda assumes the following event:

``` json
{
  "database": "glue-database-name",
  "table": "glue-table-name"
}
```

Invoking it locally uses the following arguments:

``` bash
./athena_partitioner.py glue-database-name glue-table-name
```

When invoking locally, you can set the AWS profile to use by either setting your `AWS_PROFILE` environment variable or passing in a profile with the `--profile` flag.


## Utility Scripts

There are a couple utility scripts in the `util` directory, that can help with fixing potentially broken tables or partitions.

The leading principal of these utility scripts is that after running one of them, running `athena-partitioner.py` should result in a fixed Athena table.
As such, while some of them are destructive, none of them do anything irreversible.


### `util/delete-all-partitions`

`delete-all-partitions` will iterate through all partitions that exist in a table and remove them.
In certain cases this can be more useful than deleting an entire table and recreating it, because you're not making changes to the table itself, and just want to reload the data.

``` bash
./util/delete-all-partitions <database> <table> [--profile <aws profile name>] [--dry-run]
```

### `util/delete-bad-partitions`

`delete-bad-partitions` will remove "bad" partitions, where bad is defined as partitions with their locations set to something other than an s3 path that matches the partition definition.
(ie. if the partition `[2019, 03, 04, 05]` has the location `s3://bucket/prefix/2018/01/01/01`, the partition will be deleted)

This script should be run if `athena_partitioner.py` has a number of partitions that it attempts to add on subsequent runs, that appear to be unsuccessful.
In most cases when this happens, it's due to the bad location.

``` bash
./util/delete-bad-partitions <database> <table> [--profile <aws profile name>] [--dry-run]
```

### `util/delete-missing-partitions`

When migrating data to new buckets, I found a number of partitions which didn't exist on disk (confirmed by looking at the old data locations).
These scripts will list which partitions we're unable to find on disk and delete them.

Run `delete-missing-partitions` with `--dry-run` and inspecting the partitions to be deleted manually.

``` bash
./util/list-missing-partitions <database> <table> <aws profile name>
# => if there are any missing partitions, they'll be printed in array format of [yyyy mm dd hh]
#    ex. [2019 03 21 13]

# now manually inspect them
aws --profile <aws profile name> glue get-partition --database-name <database> --table-name <table> --partition-values yyyy mm dd hh

# ex.
aws --profile my-profile glue get-partition --database-name my_database --table-name my_table --partition-values 2019 03 21 13

# => the output of this will be a big json blob describing the partition.]
#    the information we mainly care about at this point is the location,
#        .Partition.StorageDescriptor.Location
#    if you don't want to look at the rest of the output, you can add
#        --query 'Partition.StorageDescriptor.Location'
#    to the end of the get-partition command. But it can be useful to look at the full output
#    to see if there are any obvious errors with that

# now that you have the partition location, look in the s3 bucket to see if any data exists
aws --profile <aws profile name> s3 ls <location from above>

# ex.
aws --profile my-profile s3 ls s3://my-bucket/warehose/my_table/year=2019/month=03/day=12/hour=13/

# if any data exists for that partition, the files will be listed out.
# if the result of that command is nothing, start removing directories to confirm any data exists.
#
# it can be beneficial to look at a known good partition, and confirm the paths are similar.

# if you've confirmed that the partition has no data, it is safe to delete it, which you can do with
./util/delete-missing-partitions <database> <table> [--profile <aws profile name>] [--dry-run]
```

Like `delete-bad-partitions` and `delete-all-partitions`, if the table location is up to date in the glue catalog, after running `delete-missing-partitions`, the next `athena-partitioner.py` run will find and add any partitions that exist on disk.

### `util/update-partitions`

`update-partitions` was written as part of our data migration, where we moved the backing data from disparate, single-purpose s3 buckets to a single, unified s3 bucket per-environment.
It was written to avoid deleting and recreating tables as the data was moved.

To use `update-partitions`, the table's location needs to be updated to its new location (usually by running an athena `alter table <name> set location '<new s3 location>'`), and the backing data moved from the old location to the new.

How `update-partitions` functions is by grabbing all existing partitions, and comparing them against partitions that exist on disk.
The script compares the partition values (YYYY MM DD HH) with partitions found in the new S3 location, and if there are matches, updates the partition to use the new location.

``` bash
./util/update-partitions <database> <table> [--profile <aws profile name>] [--dry-run]
```


## Run Unit Tests

There are some unit tests for the Partitioner class (in `glutil/partitioner.py`) located in `tests/partitioner_test.py`.
To run these, you need to have `moto` (the aws mocking library) and `expects` (a BDD framework) installed.
These can be installed using `pip install -r requirements-dev.txt`.

At present, the Partitioner class is not fully tested.
This is because moto doesn't fully support the glue api, which we need to be able to test further parts of the partitioner well.

``` bash
# install dev requirements
pip install -r requirements-dev.txt

# one of the dev requirements is nosetests, to make running tests easier
nosetests tests

# or if you don't want to see the boto logging output on test failures (it's mostly useless)
nosetests --nologcapture tests
```
