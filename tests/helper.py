from contextlib import contextmanager
from faker import Faker
from glutil import Partition
from io import StringIO
import boto3
import random
import string
import sys


@contextmanager
def captured_output():
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err


class GlueHelper(object):
    def __init__(self, default_bucket="test-bucket", default_database="test_database", default_table="test_table"):
        self.default_bucket = default_bucket
        self.default_database = default_database
        self.default_table = default_table
        self.faker = Faker()

    def create_table_input(
            self,
            database=None,
            name=None,
            random_name=False,
            location=None):
        if not name:
            if self.default_table and not random_name:
                name = self.default_table
            else:
                to = random.randrange(5, 15)
                name = "".join(random.choice(string.ascii_lowercase)
                               for i in range(0, to))

        if not database:
            database = self.default_database

        if not location:
            path = self.faker.uri_path()
            location = "s3://{}/{}".format(self.default_bucket, path)

        return dict(
            DatabaseName=database,
            TableInput=dict(
                Name=name,
                TableType="EXTERNAL_TABLE",
                StorageDescriptor=dict(
                    Columns=[
                        dict(
                            Name="field",
                            Type="string")],
                    Compressed=False,
                    InputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    OutputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    Location=location,
                ),
                PartitionKeys=[
                    dict(Name="year", Type="int"),
                    dict(Name="month", Type="int"),
                    dict(Name="day", Type="int"),
                    dict(Name="hour", Type="int")
                ]))

    def create_database_input(self, database_name=None):
        if not database_name:
            database_name = self.default_database

        return dict(DatabaseInput=dict(
            Name=database_name,
            Description="You know, for testing"))

    def make_database_and_table(self, database_name=None, table_name=None, bucket=None, prefix=None):
        if not table_name:
            table_name = self.default_table

        if not bucket:
            bucket = self.default_bucket
        if not prefix:
            prefix = table_name

        location = f"s3://{bucket}/{prefix}/"

        client = boto3.client("glue", region_name="us-east-1")

        database_input = self.create_database_input(database_name=database_name)
        client.create_database(**database_input)

        table_input = self.create_table_input(database=database_name, name=table_name, location=location)
        client.create_table(**table_input)

    def create_partition_data(self, values=None, prefix=None, bucket=None, save=True):
        if not values:
            values = self.create_partition_values()

        if not prefix:
            prefix = self.default_table

        if prefix[0] == "/":
            prefix = prefix[1:]
        if prefix[-1] != "/":
            prefix = prefix + "/"

        if not bucket:
            bucket = self.default_bucket

        s3_key = f"{prefix}{values[0]}/{values[1]}/{values[2]}/{values[3]}/"
        location = f"s3://{bucket}/{s3_key}"

        partition = Partition(*values, location)
        if save:
            self.write_partition_to_s3(partition)

        return Partition(*values, location)

    def write_partition_to_s3(self, partition):
        location_splits = partition.location[len("s3://"):].split("/")
        bucket = location_splits[0]
        path = "/".join(location_splits[1:])

        if not path.endswith("/"):
            path += "/"
        s3_path = path + "object.json"

        s3 = boto3.client("s3", region_name="us-east-1")
        s3.put_object(
            Body='{"foo": "bar"}',
            Bucket=bucket,
            Key=s3_path)

    def create_partition_values(self):
        date = self.faker.date_between(start_date="-1y", end_date="today")
        hour = self.faker.time(pattern="%H")

        values = (
            "{:04d}".format(
                date.year), "{:02d}".format(
                date.month), "{:02d}".format(
                date.day), hour)
        return values

    def create_many_partitions(self, count=10, prefix=None, bucket=None):
        partitions = []
        for i in range(0, count):
            partition = self.create_partition_data(prefix=prefix, bucket=bucket)
            while partition in partitions:
                partition = self.create_partition_data(prefix=prefix, bucket=bucket)
            partitions.append(partition)
        return partitions
