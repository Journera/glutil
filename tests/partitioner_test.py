from unittest import TestCase
from unittest.mock import MagicMock, call, ANY
from moto import mock_s3, mock_glue
from .helper import GlueHelper
import boto3
import pendulum
import sure  # noqa: F401

from glutil import Partitioner, Partition, GlutilError


class PartitionerTest(TestCase):
    bucket = "test-bucket"
    database = "test_database"
    table = "test_table"
    region = "us-east-1"

    def setUp(self):
        super().setUp()

        self.helper = GlueHelper(
            default_bucket=self.bucket,
            default_database=self.database,
            default_table=self.table)
        self.glue = boto3.client("glue", region_name=self.region)
        self.s3 = boto3.client("s3", region_name=self.region)

    @mock_glue
    def test_init(self):
        self.helper.make_database_and_table()

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)

        partitioner.bucket.should.equal(self.bucket)
        partitioner.prefix.should.equal("test_table/")

    @mock_glue
    def test_init_no_table(self):
        with self.assertRaises(GlutilError) as context:
            Partitioner(self.database, self.table, aws_region=self.region)

        exception = context.exception
        exception.error_type.should.equal("EntityNotFound")

    @mock_glue
    @mock_s3
    def test_find_partitions_in_s3(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        partitions = self.helper.create_many_partitions(count=10)

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        found_partitions = partitioner.partitions_on_disk()

        set(found_partitions).should.equal(set(partitions))

    @mock_glue
    @mock_s3
    def test_find_partitions_in_s3_with_hive_formatted_partitions(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        # partitions = self.helper.create_many_partitions(count=10)
        partitions = []
        for i in range(1, 11):
            partition = Partition("2019", "01", f"{i:02d}", "03", f"s3://{self.bucket}/{self.table}/year=2019/month=01/day={i:02d}/hour=03/")
            print(partition.location)
            self.helper.write_partition_to_s3(partition)
            partitions.append(partition)

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        found_partitions = partitioner.partitions_on_disk()

        set(found_partitions).should.equal(set(partitions))

    @mock_glue
    @mock_s3
    def test_find_partitions_with_limit_hive_format(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        today = pendulum.now()

        partitions = []
        for i in range(1, 11):
            partition_date = today.subtract(days=i)
            year = partition_date.strftime("%Y")
            month = partition_date.strftime("%m")
            day = partition_date.strftime("%d")
            hour = "03"

            partition = Partition(year, month, day, hour, f"s3://{self.bucket}/{self.table}/year={year}/month={month}/day={day}/hour={hour}/")
            self.helper.write_partition_to_s3(partition)
            partitions.append(partition)

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        found_partitions = partitioner.partitions_on_disk(limit_days=7)
        found_partitions.should.have.length_of(7)
        set(found_partitions).should.equal(set(partitions[0:7]))

    @mock_glue
    @mock_s3
    def test_find_partitions_with_limit_flat_format(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        today = pendulum.now()

        partitions = []
        for i in range(1, 11):
            partition_date = today.subtract(days=i)
            year = partition_date.strftime("%Y")
            month = partition_date.strftime("%m")
            day = partition_date.strftime("%d")
            hour = "03"

            partition = Partition(year, month, day, hour, f"s3://{self.bucket}/{self.table}/{year}/{month}/{day}/{hour}/")
            self.helper.write_partition_to_s3(partition)
            partitions.append(partition)

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        found_partitions = partitioner.partitions_on_disk(limit_days=4)
        found_partitions.should.have.length_of(4)
        set(found_partitions).should.equal(set(partitions[0:4]))

    @mock_glue
    @mock_s3
    def test_find_partitions_with_bad_limit(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        partitioner.partitions_on_disk.when.called_with(limit_days=-1).should.throw(ValueError)

    @mock_glue
    @mock_s3
    def test_create_partitions(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        partition = self.helper.create_partition_data()

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)

        create_partitions_mock = MagicMock(return_value=[])
        partitioner.glue.batch_create_partition = create_partitions_mock

        partitioner.create_partitions([partition])

        create_partitions_mock.assert_called_with(
            DatabaseName=self.database,
            TableName=self.table,
            PartitionInputList=[partitioner._partition_input(partition)])

    @mock_s3
    @mock_glue
    def test_create_partition_when_partition_exists(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        partition = self.helper.create_partition_data()

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)

        create_partitions_mock = MagicMock(return_value={
            "Errors": [{
                "PartitionValues": partition.values,
                "ErrorDetail": {
                    "ErrorCode": "AlreadyExistsException",
                    "ErrorMessage": "Partition already exists"
                }}]
        })
        partitioner.glue.batch_create_partition = create_partitions_mock

        errors = partitioner.create_partitions([partition])

        create_partitions_mock.assert_called_once()
        errors.should.have.length_of(1)
        errors[0]["PartitionValues"].should.equal(partition.values)
        errors[0]["ErrorDetail"]["ErrorCode"].should.equal(
            "AlreadyExistsException")

    @mock_s3
    @mock_glue
    def test_create_partition_batches_by_one_hundred(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        partitions = sorted(self.helper.create_many_partitions(count=150))
        partitioner = Partitioner(self.database, self.table, aws_region=self.region)

        create_partitions_mock = MagicMock(return_value=[])
        partitioner.glue.batch_create_partition = create_partitions_mock

        partitioner.create_partitions(partitions)

        first_list = [partitioner._partition_input(p) for p in partitions[:100]]
        second_list = [partitioner._partition_input(p) for p in partitions[100:]]
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

    @mock_s3
    @mock_glue
    def test_create_partition_already_exists_in_multiple_batches(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        partitions = sorted(self.helper.create_many_partitions(count=150))
        partitioner = Partitioner(self.database, self.table, aws_region=self.region)

        # prime partitions list with two partitions, one in each group
        already_exists = [partitions[5], partitions[115]]
        errors = partitioner.create_partitions(already_exists)
        errors.should.be.empty

        # now attempt to create them as part of a large batch
        errors = partitioner.create_partitions(partitions)
        errors.should.have.length_of(2)

        for idx, error in enumerate(errors):
            partition = already_exists[idx]
            error["PartitionValues"].should.equal(partition.values)
            error["ErrorDetail"]["ErrorCode"].should.equal("AlreadyExistsException")

    @mock_s3
    @mock_glue
    def test_create_partitions_on_disk_with_bad_table_location(self):
        self.s3.create_bucket(Bucket=self.bucket)
        database_input = self.helper.create_database_input()
        self.glue.create_database(**database_input)

        # no trailing slash for location is on purpose and what this
        # test is checking against
        table_input = self.helper.create_table_input(location=f"s3://{self.bucket}/{self.table}")
        self.glue.create_table(**table_input)

        partition = self.helper.create_partition_data()
        full_location = f"s3://{self.bucket}/{self.table}/{partition.year}/{partition.month}/{partition.day}/{partition.hour}/"

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)

        found_partitions = partitioner.partitions_on_disk()

        found_partitions.should.have.length_of(1)
        found_partitions[0].location.should.equal(full_location)

    @mock_s3
    @mock_glue
    def test_find_partitions_in_glue_catalog(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        partition = self.helper.create_partition_data()
        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        partitioner.create_partitions([partition])

        existing_partitions = partitioner.existing_partitions()
        existing_partitions.should.have.length_of(1)
        existing_partitions[0].values.should.equal(partition.values)
        existing_partitions[0].location.should.equal(partition.location)

    @mock_s3
    @mock_glue
    def test_partitions_to_create(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        already_created = self.helper.create_many_partitions(count=10, write=True)
        to_create = self.helper.create_many_partitions(count=3, write=True)

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        partitioner.create_partitions(already_created)

        found = partitioner.partitions_on_disk()
        wants_to_create = partitioner.partitions_to_create(found)

        set(wants_to_create).should.equal(set(to_create))

    @mock_s3
    @mock_glue
    def test_partitions_to_create_empty_input(self):
        # this is to test the early exit
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        wants_to_create = partitioner.partitions_to_create([])
        wants_to_create.should.have.length_of(0)

    @mock_s3
    @mock_glue
    def test_delete_partitions(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()
        self.helper.create_partition_data()

        partition = self.helper.create_partition_data()
        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        partitioner.create_partitions([partition])

        mock = MagicMock(return_value=[])
        partitioner.glue.batch_delete_partition = mock

        to_delete = partitioner.existing_partitions()
        partitioner.delete_partitions(to_delete)

        mock.assert_called_with(
            DatabaseName=self.database,
            TableName=self.table,
            PartitionsToDelete=[{"Values": to_delete[0].values}]
        )

    @mock_s3
    @mock_glue
    def test_delete_partitions_in_groups_of_twenty_five(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        partitions = sorted(self.helper.create_many_partitions(count=30))

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        partitioner.create_partitions(partitions)

        mock = MagicMock(return_value=[])
        partitioner.glue.batch_delete_partition = mock

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

    @mock_s3
    @mock_glue
    def test_delete_nonexistent_partition(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()
        partitioner = Partitioner(self.database, self.table, aws_region=self.region)

        partition = Partition("2019", "01", "02", "03", f"s3://{self.bucket}/{self.table}/2019/01/02/03/")

        result = partitioner.delete_partitions([partition])
        result.should.have.length_of(1)
        result[0]["PartitionValues"].should.equal(["2019", "01", "02", "03"])
        result[0]["ErrorDetail"]["ErrorCode"].should.equal("EntityNotFoundException")

    @mock_s3
    @mock_glue
    def test_bad_partitions(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        partition = self.helper.create_partition_data()
        self.glue.create_partition(
            DatabaseName=self.database,
            TableName=self.table,
            PartitionInput={
                "Values": partition.values,
                "StorageDescriptor": {
                    "Location": "s3://not-a-bucket/who-cares/"
                },
            })

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        bad_partitions = partitioner.bad_partitions()

        bad_partitions.should.have.length_of(1)
        bad_partitions[0].values.should.equal(partition.values)
        bad_partitions[0].location.should.equal("s3://not-a-bucket/who-cares/")

    @mock_s3
    @mock_glue
    def test_missing_partitions(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        self.glue.create_partition(
            DatabaseName=self.database,
            TableName=self.table,
            PartitionInput={
                "Values": ["2019", "01", "02", "03"],
                "StorageDescriptor": {
                    "Location": "s3://not-a-bucket/who-cares/"
                },
            })

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        missing_partitions = partitioner.missing_partitions()

        missing_partitions.should.have.length_of(1)
        missing_partitions[0].values.should.equal(["2019", "01", "02", "03"])

    @mock_s3
    @mock_glue
    def test_find_moved_partitions(self):
        old_location = "s3://old-bucket/table/"

        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        partitions = sorted(self.helper.create_many_partitions(count=15))

        batch_input = []
        for partition in partitions:
            batch_input.append({
                "Values": partition.values,
                "StorageDescriptor": {
                    "Location": f"{old_location}/data/"
                }
            })

        self.glue.batch_create_partition(
            DatabaseName=self.database,
            TableName=self.table,
            PartitionInputList=batch_input)

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        moved = partitioner.find_moved_partitions()

        moved.should.have.length_of(len(partitions))

        moved.sort()
        partitions.sort()

        for idx, partition in enumerate(partitions):
            moved[idx].should.equal(partition)

    @mock_s3
    @mock_glue
    def test_find_moved_partitions_with_missing_partitions(self):
        old_location = "s3://old-bucket/table/"

        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        self.glue.create_partition(
            DatabaseName=self.database,
            TableName=self.table,
            PartitionInput={
                "Values": ["2019", "01", "01", "01"],
                "StorageDescriptor": {"Location": f"{old_location}/data/"}
            })

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        mock = MagicMock()
        partitioner.glue.update_partition = mock

        updated = partitioner.find_moved_partitions()
        updated.should.be.empty

    @mock_s3
    @mock_glue
    def test_update_partition_locations(self):
        old_location = "s3://old-bucket/table/"

        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        partitions = sorted(self.helper.create_many_partitions(count=15))

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

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        mock = MagicMock()
        partitioner.glue.update_partition = mock

        moved = partitioner.find_moved_partitions()
        errors = partitioner.update_partition_locations(moved)

        errors.should.be.empty
        mock.assert_has_calls(calls, any_order=True)

    @mock_glue
    def test_update_partition_locations_with_non_existent_partition(self):
        self.helper.make_database_and_table()
        bad_partition = Partition("2019", "01", "01", "01", "s3://who/cares/")

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        mock = MagicMock()
        partitioner.glue.update_partition = mock

        errors = partitioner.update_partition_locations([bad_partition])
        errors.should.have.length_of(1)
        errors[0]["Partition"].should.equal(bad_partition.values)
        mock.assert_not_called()

    @mock_glue
    def test_update_partition_locations_with_mix_of_good_and_bad(self):
        self.helper.make_database_and_table()

        good_old_location = "s3://old-bucket/table/data1/"
        good_new_location = f"s3://{self.bucket}/{self.table}/2019-01-01-01/"
        good_partition = Partition("2019", "01", "01", "01", good_old_location)
        bad_partition = Partition("2018", "02", "02", "02", "s3://old-bucket/table/data2/")

        self.glue.create_partition(
            DatabaseName=self.database,
            TableName=self.table,
            PartitionInput={
                "Values": good_partition.values,
                "StorageDescriptor": {"Location": good_partition.location}
            })

        good_partition.location = good_new_location

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        mock = MagicMock()
        partitioner.glue.update_partition = mock

        errors = partitioner.update_partition_locations([bad_partition, good_partition])

        mock.assert_called_with(
            DatabaseName=self.database,
            TableName=self.table,
            PartitionValueList=good_partition.values,
            PartitionInput={
                "Values": good_partition.values,
                "StorageDescriptor": {"Location": good_new_location}
            })

        errors.should.have.length_of(1)
        errors[0]["Partition"].should.equal(bad_partition.values)


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

    def test_parse_from_aws(self):
        normal_aws_response = {
            "Values": ["2019", "01", "02", "03"],
            "StorageDescriptor": {
                "Location": "s3://bucket/location/2019/01/02/03/",
            },
        }

        partition = Partition.from_aws_response(normal_aws_response)
        partition.year.should.equal("2019")
        partition.month.should.equal("01")
        partition.day.should.equal("02")
        partition.hour.should.equal("03")
        partition.location.should.equal("s3://bucket/location/2019/01/02/03/")

        # Conform location gets normalized by Partition
        bad_location_aws_response = {
            "Values": ["2019", "01", "02", "03"],
            "StorageDescriptor": {
                "Location": "s3://bucket/location/2019/01/02/03",
            },
        }
        partition2 = Partition.from_aws_response(bad_location_aws_response)
        partition2.year.should.equal("2019")
        partition2.month.should.equal("01")
        partition2.day.should.equal("02")
        partition2.hour.should.equal("03")
        partition2.location.should.equal("s3://bucket/location/2019/01/02/03/")

        partition2.should.equal(partition)
