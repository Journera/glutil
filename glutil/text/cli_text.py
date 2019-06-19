create_partitions_help = """
Create partitions based on S3 data.

This will crawl S3 for anything that looks like a partition underneath the
table's location, and create new partitions in the catalog for those found
locations.

For example, if your table has the location of
    s3://some-bucket/some-table/

And you have data stored at
    s3://some-bucket/some-table/2019/01/02/03/, or
    s3://some-bucket/some-table/year=2019/month=01/day=02/hour=03/

This script will create a partition with the values [2019 01 02 03].
"""


delete_all_partitions_help = """
Delete all partitions in a table.

This is useful should your partitions somehow get screwed up. Ideally, after
running this, you should run 'create-partitions'.

NOTE: This will be substantially slower than deleting and recreating a table,
and in most cases you should just do that instead of running this script.
"""


delete_bad_partitions_help = """
Delete "bad" partitions in a table.

"Bad" partitions are those where the S3 location does not match the expected
location. This can happen for a variety of reasons, with the most common being
moving the backing data to a new S3 location.

For example, if your table has the location of
    s3://some-bucket/some-table/

And you have a partition with the location of
    s3://some-other-bucket/who-knows/YYYY/MM/DD/HH/

This script will delete that partition.

It will also delete partitions who's S3 location values do not match its
partition values (for example, a if a partition has the values
[2019, 01, 01, 01], but is located in s3://some-bucket/table/2019/02/02/02/).

After running this you should run the partitioner again to pick up the correct
location of any deleted partitions.
"""


delete_bad_tables_help = """
Delete bad tables.

What is a bad table?

1.  Any table that exists in a location _under_ another table.
    For example, if one table is located in
        s3://some-bucket/some-directory/
    and another table is found located in
        s3://some-bucket/some-directory/another-directory/
    the second table (another-directory) will be deleted.

2.  When two tables exist in the same location, and one is named after the
    location, while the other is named location_somerandomcharacters, the second
    is considered bad and deleted.

Most of the time, these tables are created by the Glue Crawler doing something
we didn't want or expect.
"""


delete_missing_partitions_help = """
Delete missing partitions in a table.

This will remove any partitions currently in the database that do not exist on
disk.
"""


update_partitions_help = """
Update partition locations.

If the underlying location of the data is moved, and the table's location is
updated to match, this script will attempt to match known partitions with
partitions found in the new location, and update the corresponding partitions
with new locations.

NOTE: this does not move the data, it will only update partitions found in the
new location. You must move the data and update the table's location yourself,
outside of this script.
"""
