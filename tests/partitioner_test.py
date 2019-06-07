from unittest import TestCase
from unittest.mock import MagicMock, call, ANY
from moto import mock_s3, mock_glue
import boto3
import botocore
from faker import Faker
import random
import sure

from glutil import Partitioner, Partition, GlutilError

faker = Faker()


@mock_s3
@mock_glue
class PartitionerTest(TestCase):
    bucket = "test-bucket"
    database = "test_database"
    table = "test_table"

    partition_keys = [
        dict(Name="year", Type="int"),
        dict(Name="month", Type="int"),
        dict(Name="day", Type="int"),
        dict(Name="hour", Type="int"),
    ]
    storage_descriptor = dict(
        Columns=[dict(Name="field", Type="string")],
        Compressed=False,
        InputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        OutputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        Location="s3://test-bucket/test_table/",
    )
    table_type = "EXTERNAL_TABLE"

    def setUp(self):
        super().setUp()

        self.glue = boto3.client("glue")
        self.s3 = boto3.client("s3")

        self.glue.create_table(
            DatabaseName=self.database,
            TableInput=dict(
                Name=self.table,
                StorageDescriptor=self.storage_descriptor,
                PartitionKeys=self.partition_keys,
                TableType=self.table_type,
            ),
        )

    @classmethod
    @mock_glue
    @mock_s3
    def setUpClass(cls):
        """
        This method (and all the setup/teardown stuff in this class right now) is kinda hacky.
        This is because moto, the aws mocker, doesn't fully support glue, so a bunch of things
        are in this state until it's fixed.
        """
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=cls.bucket)

        glue = boto3.client("glue")
        glue.create_database(
            DatabaseInput=dict(
                Name=cls.database,
                Description="You know, for testing",
            ),
        )

    def tearDown(self):
        super().tearDown()

        s3 = boto3.resource("s3")
        s3.Bucket(self.bucket).objects.all().delete()
        self.glue.delete_table(DatabaseName=self.database, Name=self.table)

    def create_fake_object(self, partition):
        path = "{}/{}/{}/{}/{}/".format(self.table,
                                        partition[0],
                                        partition[1],
                                        partition[2],
                                        partition[3])
        key = f"{path}object.json"
        self.s3.put_object(
            Body="idk it doesn't matter",
            Bucket=self.bucket,
            Key=key,
        )

        location = "s3://{}/{}".format(self.bucket, path)
        return location

    def create_fake_data(self, count=None):
        if count:
            range_count = count
        else:
            range_count = random.randrange(1, 10)

        fake_partitions = []
        partition_values = []
        for i in range(0, range_count):
            values = self._generate_fake_values()
            while values in partition_values:
                values = self._generate_fake_values()

            partition_values.append(values)

            location = self.create_fake_object(values)
            fake_partitions.append(Partition(*values, location))

        return fake_partitions

    def _generate_fake_values(self):
        date = faker.date_between(start_date="-1y", end_date="today")
        hour = faker.time(pattern="%H")

        values = (
            "{:04d}".format(
                date.year), "{:02d}".format(
                date.month), "{:02d}".format(
                date.day), hour)
        return values

    def test_init(self):
        partitioner = Partitioner(self.database, self.table)

        partitioner.storage_descriptor.should.equal(self.storage_descriptor)
        partitioner.bucket.should.equal(self.bucket)
        partitioner.prefix.should.equal("test_table/")

    def test_find_partitions_in_s3(self):
        partitions = set(self.create_fake_data())

        partitioner = Partitioner(self.database, self.table)
        found_partitions = partitioner.partitions_on_disk()

        found_partitions_by_values = {
            tuple(p.values) for p in found_partitions}
        created_partitions_by_values = {tuple(p.values) for p in partitions}

        set(found_partitions_by_values).should.equal(
            set(created_partitions_by_values))

    def test_create_new_partition(self):
        partition = self.create_fake_data(count=1)[0]
        partitioner = Partitioner(self.database, self.table)

        create_partitions_mock = MagicMock(return_value=[])
        partitioner.glue.batch_create_partition = create_partitions_mock

        partitioner.create_new_partitions()

        create_partitions_mock.assert_called_with(
            DatabaseName=self.database,
            TableName=self.table,
            PartitionInputList=[partitioner._partition_input(partition)])

    def test_create_new_partition_when_partition_exists(self):
        partition = self.create_fake_data(count=1)[0]
        self.glue.create_partition(
            DatabaseName=self.database,
            TableName=self.table,
            PartitionInput={
                "Values": partition.values,
                "StorageDescriptor": {"Location": partition.location}})

        partitioner = Partitioner(self.database, self.table)

        create_partitions_mock = MagicMock(return_value=[])
        partitioner.glue.batch_create_partition = create_partitions_mock

        partitioner.create_new_partitions()

        create_partitions_mock.assert_not_called()

    def test_create_new_partition_when_creation_race(self):
        """Test the case when someone creates a partition that
        should be created by the partitioner between the partitioner
        getting a list of existing partitions and attempting to
        create the partition"""
        partition = self.create_fake_data(count=1)[0]

        partitioner = Partitioner(self.database, self.table)

        create_partitions_mock = MagicMock(return_value={
            "Errors": [{
                "PartitionValues": partition.values,
                "ErrorDetail": {
                    "ErrorCode": "AlreadyExistsException",
                    "ErrorMessage": "Partition already exists"
                }}]
        })
        partitioner.glue.batch_create_partition = create_partitions_mock

        errors = partitioner.create_new_partitions()
        print(errors)

        create_partitions_mock.assert_called_once()
        errors.should.have.length_of(1)
        errors[0]["PartitionValues"].should.equal(partition.values)
        errors[0]["ErrorDetail"]["ErrorCode"].should.equal(
            "AlreadyExistsException")

    def test_create_new_partition_batches_by_one_hundred(self):
        partitions = sorted(self.create_fake_data(count=150))
        partitioner = Partitioner(self.database, self.table)

        create_partitions_mock = MagicMock(return_value=[])
        partitioner.glue.batch_create_partition = create_partitions_mock

        partitioner.create_new_partitions()

        first_list = [partitioner._partition_input(
            p) for p in partitions[:100]]
        second_list = [partitioner._partition_input(
            p) for p in partitions[100:]]
        calls = [
            call(
                DatabaseName=self.database,
                TableName=self.table,
                PartitionInputList=first_list),
            call(
                DatabaseName=self.database,
                TableName=self.table,
                PartitionInputList=second_list),
        ]

        create_partitions_mock.call_count.should.equal(2)
        create_partitions_mock.assert_has_calls(calls)

    def test_find_partitions_in_glue_catalog(self):
        partition = self.create_fake_data(count=1)[0]
        partitioner = Partitioner(self.database, self.table)
        partitioner.create_new_partitions()

        existing_partitions = partitioner.existing_partitions()
        existing_partitions.should.have.length_of(1)
        existing_partitions[0].values.should.equal(partition.values)
        existing_partitions[0].location.should.equal(partition.location)

    def test_delete_partitions(self):
        partition = self.create_fake_data(count=1)[0]
        partitioner = Partitioner(self.database, self.table)
        partitioner.create_new_partitions()

        mock = MagicMock(return_value=[])
        partitioner.glue.batch_delete_partitions = mock

        to_delete = partitioner.existing_partitions()
        partitioner.delete_partitions(to_delete)

        mock.assert_called_with(
            DatabaseName=self.database,
            TableName=self.table,
            PartitionsToDelete=[{"Values": to_delete[0].values}]
        )

    def test_delete_partitions_in_groups_of_twenty_five(self):
        partitions = sorted(self.create_fake_data(count=30))

        partitioner = Partitioner(self.database, self.table)
        partitioner.create_new_partitions()

        mock = MagicMock(return_value=[])
        partitioner.glue.batch_delete_partitions = mock

        existing_partitions = partitioner.existing_partitions()
        partitioner.delete_partitions(existing_partitions)

        first_list = [{"Values": p.values} for p in partitions[:25]]
        second_list = [{"Values": p.values} for p in partitions[25:]]
        calls = [
            call(
                DatabaseName=self.database,
                TableName=self.table,
                PartitionsToDelete=first_list),
            call(
                DatabaseName=self.database,
                TableName=self.table,
                PartitionsToDelete=second_list),
        ]

        mock.call_count.should.equal(2)
        mock.assert_has_calls(calls)

    def test_bad_partitions(self):
        partition = self.create_fake_data(count=1)[0]
        self.glue.create_partition(
            DatabaseName=self.database,
            TableName=self.table,
            PartitionInput={
                "Values": partition.values,
                "StorageDescriptor": {
                    "Location": "s3://not-a-bucket/who-cares/"
                },
            })

        partitioner = Partitioner(self.database, self.table)
        bad_partitions = partitioner.bad_partitions()

        bad_partitions.should.have.length_of(1)
        bad_partitions[0].values.should.equal(partition.values)
        bad_partitions[0].location.should.equal("s3://not-a-bucket/who-cares/")

    def test_missing_partitions(self):
        self.glue.create_partition(
            DatabaseName=self.database,
            TableName=self.table,
            PartitionInput={
                "Values": ["2019", "01", "02", "03"],
                "StorageDescriptor": {
                    "Location": "s3://not-a-bucket/who-cares/"
                },
            })

        partitioner = Partitioner(self.database, self.table)
        missing_partitions = partitioner.missing_partitions()

        missing_partitions.should.have.length_of(1)
        missing_partitions[0].values.should.equal(["2019", "01", "02", "03"])

    def test_update_moved_partitions(self):
        old_location = "s3://old-bucket/table/"
        partitions = self.create_fake_data()

        batch_input = []
        calls = []
        for partition in partitions:
            batch_input.append({
                "Values": partition.values,
                "StorageDescriptor": {
                    "Location": f"{old_location}/data/"
                }
            })

            calls.append(call(
                DatabaseName=self.database,
                TableName=self.table,
                PartitionValueList=partition.values,
                PartitionInput=ANY))

        self.glue.batch_create_partition(
            DatabaseName=self.database,
            TableName=self.table,
            PartitionInputList=batch_input)

        partitioner = Partitioner(self.database, self.table)
        mock = MagicMock()
        partitioner.glue.update_partition = mock

        partitioner.update_moved_partitions()

        mock.assert_has_calls(calls, any_order=True)


class PartitionTest(TestCase):
    def test_partition_comparisons(self):
        p1 = Partition("2019", "01", "01", "01", "s3://bucket/table/")
        p2 = Partition("2019", "02", "02", "02", "s3://bucket/table2/")
        (p1 > p2).should.be.false
        (p1 < p2).should.be.true

        p3 = Partition("2019", "01", "01", "01", "s3://bucket/table/")
        (p1 == p3).should.be.true
        p1._cmp(p3).should.equal(0)

        p4 = Partition("2019", "01", "01", "01", "s3://bucket/z-table/")
        (p1 > p4).should.be.true
        (p4 > p1).should.be.false
