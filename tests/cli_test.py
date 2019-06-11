from unittest import TestCase
from unittest.mock import MagicMock
from moto import mock_s3, mock_glue
from .helper import GlueHelper, captured_output
import boto3
import sure  # noqa: F401

from glutil import Cli, Partitioner, DatabaseCleaner
from glutil.database_cleaner import Table
from glutil.partitioner import PartitionMap


class CliTest(TestCase):
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

        self.s3 = boto3.client("s3")
        self.glue = boto3.client("glue")

    def get_cmd_output(self, cli, cli_args):
        with captured_output() as (out, err):
            cli.main(cli_args)
        output = out.getvalue().strip()
        error = err.getvalue().strip()

        return output, error

    @mock_glue
    @mock_s3
    def test_delete_all_partitions(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()
        cli = Cli()

        # case without any partitions
        out, err = self.get_cmd_output(cli, ["delete-all-partitions", self.database, self.table])
        out.should.equal("No partitions found in table test_table")

        # dry run with partitions
        partitions = self.helper.create_many_partitions(count=10)
        partitions.sort()
        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        partitioner.create_partitions(partitions)

        expected_out = "Deleting the following partitions:"
        for partition in partitions:
            expected_out += f"\n\t{str(partition)}"

        out, err = self.get_cmd_output(cli, ["delete-all-partitions", self.database, self.table, "--dry-run"])
        out.should.equal(expected_out)

        found_partitions = partitioner.existing_partitions()
        found_partitions.should.have.length_of(len(partitions))

        # case with partitions
        out, err = self.get_cmd_output(cli, ["delete-all-partitions", self.database, self.table])
        out.should.equal(expected_out)

        found_partitions = partitioner.existing_partitions()
        found_partitions.should.have.length_of(0)

        # test error output
        partition = self.helper.create_partition_data()
        partitioner.create_partitions([partition])
        mock = MagicMock()
        mock.return_value = [{
            "PartitionValues": partition.values,
            "ErrorDetail": {
                "ErrorCode": "PartitionNotFound",
                "ErrorMessage": "Partition not found"
            }
        }]
        partitioner.delete_partitions = mock
        partitioner_mock = MagicMock(return_value=partitioner)
        cli.get_partitioner = partitioner_mock

        expected_output = f"Deleting the following partitions:\n\t{partition}\nOne or more errors occurred when attempting to delete partitions\nError on {partition.values}: PartitionNotFound"
        out, err = self.get_cmd_output(cli, ["delete-all-partitions", self.database, self.table])
        out.should.equal(expected_output)

    @mock_glue
    @mock_s3
    def test_delete_bad_partitions(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()
        cli = Cli()

        # case with no partitions
        out, err = self.get_cmd_output(cli, ["delete-bad-partitions", self.database, self.table])
        out.should.equal("Found 0 partitions to delete")

        # dry run with partitions
        partitions = self.helper.create_many_partitions(count=10, prefix="not-this-table")
        partitions.sort()

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        partitioner.create_partitions(partitions)

        expected_out = "Found 10 partitions to delete\nDeleting the following partitions:"
        for partition in partitions:
            expected_out += f"\n\t{str(partition)}"

        out, err = self.get_cmd_output(cli, ["delete-bad-partitions", self.database, self.table, "--dry-run"])
        out.should.equal(expected_out)

        found_partitions = partitioner.existing_partitions()
        found_partitions.should.have.length_of(10)

        # case with partitions
        out, err = self.get_cmd_output(cli, ["delete-bad-partitions", self.database, self.table])
        out.should.equal(expected_out)

        found_partitions = partitioner.existing_partitions()
        found_partitions.should.have.length_of(0)

        # test error output
        partition = self.helper.create_partition_data(prefix="not-this-table")
        partitioner.create_partitions([partition])
        mock = MagicMock()
        mock.return_value = [{
            "PartitionValues": partition.values,
            "ErrorDetail": {
                "ErrorCode": "PartitionNotFound",
                "ErrorMessage": "Partition not found"
            }
        }]
        partitioner.delete_partitions = mock
        partitioner_mock = MagicMock(return_value=partitioner)
        cli.get_partitioner = partitioner_mock

        expected_output = f"Found 1 partitions to delete\nDeleting the following partitions:\n\t{partition}\nOne or more errors occurred when attempting to delete partitions\nError on {partition.values}: PartitionNotFound"
        out, err = self.get_cmd_output(cli, ["delete-bad-partitions", self.database, self.table])
        out.should.equal(expected_output)

    @mock_glue
    def test_delete_bad_tables(self):
        database_input = self.helper.create_database_input()
        self.glue.create_database(**database_input)
        cli = Cli()

        # create root table
        location = "s3://bucket/root-table/"
        root_table_input = self.helper.create_table_input(location=location)
        self.glue.create_table(**root_table_input)

        # base condition - only root table
        out, err = self.get_cmd_output(cli, ["delete-bad-tables", self.database])
        out.should.equal("Nothing to delete")

        # case with multiple child tables - dry run
        tables = []
        for i in range(1, 12):
            tbl_location = f"{location}{i}/"
            tbl_input = self.helper.create_table_input(location=tbl_location, random_name=True)
            self.glue.create_table(**tbl_input)
            tbl_input['TableInput']["DatabaseName"] = self.database
            tables.append(Table(tbl_input['TableInput']))

        expected_output = "Going to delete the following tables:"
        tables.sort(key=lambda x: x.name)
        for table in tables:
            expected_output += f"\n\t{table}"

        out, err = self.get_cmd_output(cli, ["delete-bad-tables", self.database, "--dry-run"])
        cleaner = DatabaseCleaner(self.database, aws_region=self.region)
        out.should.equal(expected_output)
        found_tables = cleaner.child_tables()
        found_tables.sort(key=lambda x: x.name)
        found_tables.should.equal(tables)

        # for reals now
        out, err = self.get_cmd_output(cli, ["delete-bad-tables", self.database])
        out.should.equal(expected_output)

        cleaner.refresh_trees()
        found_tables = cleaner.child_tables()
        found_tables.should.have.length_of(0)

        # test error output
        table_input = self.helper.create_table_input(location=location, name=f"test_table-bazer")
        self.glue.create_table(**table_input)

        mock = MagicMock()
        mock.return_value = [{
            "TableName": "test_table-bazer",
            "ErrorDetail": {
                "ErrorCode": "EntityNotFoundException",
                "ErrorMessage": "Table not found",
            },
        }]
        cleaner.refresh_trees()
        cleaner.delete_tables = mock
        cleaner_mock = MagicMock(return_value=cleaner)
        cli.get_database_cleaner = cleaner_mock

        expected_output = f"Going to delete the following tables:\n\t<Table {self.database} / test_table-bazer : {location}>\nOne or more errors occurred when attempting to delete tables\nError on test_table-bazer: EntityNotFoundException"
        out, err = self.get_cmd_output(cli, ["delete-bad-tables", self.database])
        mock.assert_called()
        out.should.equal(expected_output)


    @mock_glue
    @mock_s3
    def test_delete_missing_partitions(self):
        self.helper.make_database_and_table()
        cli = Cli()

        self.s3.create_bucket(Bucket=self.bucket)
        partitions = self.helper.create_many_partitions(count=10)
        partitions.sort()

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        partitioner.create_partitions(partitions)

        # no partitions to delete
        out, err = self.get_cmd_output(cli, ["delete-missing-partitions", self.database, self.table])
        out.should.equal("Found 0 partitions to delete:")

        # with partitions to delete, dry run
        s3resource = boto3.resource("s3")
        bucket = s3resource.Bucket(self.bucket)
        for obj in bucket.objects.all():
            obj.delete()

        expected_out = "Found 10 partitions to delete:"
        for partition in partitions:
            expected_out += f"\n\t{partition}"

        out, err = self.get_cmd_output(cli, ["delete-missing-partitions", self.database, self.table, "--dry-run"])
        out.should.equal(expected_out)

        found_partitions = partitioner.existing_partitions()
        found_partitions.should.have.length_of(10)
        set(found_partitions).should.equal(set(partitions))

        # no dry run
        out, err = self.get_cmd_output(cli, ["delete-missing-partitions", self.database, self.table])
        out.should.equal(expected_out)

        found_partitions = partitioner.existing_partitions()
        found_partitions.should.have.length_of(0)

        # test error output
        partition = self.helper.create_partition_data(save=False)
        partitioner.create_partitions([partition])
        mock = MagicMock()
        mock.return_value = [{
            "PartitionValues": partition.values,
            "ErrorDetail": {
                "ErrorCode": "PartitionNotFound",
                "ErrorMessage": "Partition not found"
            }
        }]
        partitioner.delete_partitions = mock
        partitioner_mock = MagicMock(return_value=partitioner)
        cli.get_partitioner = partitioner_mock

        expected_output = f"Found 1 partitions to delete:\n\t{partition}\nOne or more errors occurred when attempting to delete partitions\nError on {partition.values}: PartitionNotFound"
        out, err = self.get_cmd_output(cli, ["delete-missing-partitions", self.database, self.table])
        out.should.equal(expected_output)

    @mock_s3
    @mock_glue
    def test_update_partitions(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()
        cli = Cli()

        partitions = self.helper.create_many_partitions(10)
        partitions.sort()
        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        partitioner.create_partitions(partitions)

        # all partitions correctly located
        out, err = self.get_cmd_output(cli, ["update-partitions", self.database, self.table])
        out.should.equal("No partitions to update")

        # some partitions moved
        expected_output = "Found 5 moved partitions"

        partitions_to_move = partitions[0:5]
        for p in partitions_to_move:
            new_location = f"s3://old-bucket/old-table/{p.year}/{p.month}/{p.day}/{p.hour}/"
            p.location = new_location
            expected_output += f"\n\t{p}"

        partitioner.update_partition_locations(partitions_to_move)

        # test dry run
        out, err = self.get_cmd_output(cli, ["update-partitions", self.database, self.table, "--dry-run"])
        out.should.equal(expected_output)

        found_map = PartitionMap(partitioner.existing_partitions())
        for partition in partitions_to_move:
            matching = found_map.get(partition)
            matching.should_not.be.false
            matching.location.startswith(f"s3://{self.bucket}/{self.table}/").should.be.false

        # test for real
        out, err = self.get_cmd_output(cli, ["update-partitions", self.database, self.table])
        out.should.equal(expected_output)

        found_map = PartitionMap(partitioner.existing_partitions())
        for partition in partitions_to_move:
            matching = found_map.get(partition)
            matching.should_not.be.false
            matching.location.startswith(f"s3://{self.bucket}/{self.table}/").should.be.true

        # test error condition
        partition = self.helper.create_partition_data()
        partition.location = "s3://old-bucket/old-table/"
        partitioner.create_partitions([partition])

        mock = MagicMock()
        mock.return_value = [{
            "PartitionValues": partition.values,
            "ErrorDetail": {
                "ErrorCode": "PartitionNotFound",
                "ErrorMessage": "Partition not found"
            }
        }]
        partitioner.update_partition_locations = mock

        partitioner_mock = MagicMock(return_value=partitioner)
        cli.get_partitioner = partitioner_mock

        expected_output = f"Found 1 moved partitions\n\t{partition}\nOne or more errors occurred when attempting to update partitions\nError on {partition.values}: PartitionNotFound"
        out, err = self.get_cmd_output(cli, ["update-partitions", self.database, self.table])
        out.should.equal(expected_output)
