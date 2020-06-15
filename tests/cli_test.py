from unittest import TestCase
from unittest.mock import MagicMock
from moto import mock_s3, mock_glue
from .helper import GlueHelper, captured_output
from collections import namedtuple
import boto3
import pendulum
import sure  # noqa: F401
import sys

from glutil import Cli, Partitioner, Partition, DatabaseCleaner, GlutilError
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

        self.exit_mock = MagicMock()
        self.original_exit = sys.exit
        sys.exit = self.exit_mock

    def tearDown(self):
        sys.exit = self.original_exit
        super().tearDown()

    def get_cmd_output(self, cli, cli_args):
        with captured_output() as (out, err):
            cli.main(cli_args)
        output = out.getvalue().strip()
        error = err.getvalue().strip()

        return output, error

    @mock_glue
    @mock_s3
    def test_create_partitions(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()
        cli = Cli()

        partitions = self.helper.create_many_partitions(count=10)
        partitions.sort()

        expected_output = f"Running Partitioner for {self.database}.{self.table}\n\tLooking for partitions in s3://{self.bucket}/{self.table}/\n\tFound 10 new partitions to create\n\t"
        expected_output += ", ".join(map(str, partitions))

        out, err = self.get_cmd_output(cli, ["create-partitions", self.database, self.table])
        out.should.equal(expected_output)

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        found = partitioner.partitions_on_disk()

        set(found).should.equal(set(partitions))

    @mock_glue
    @mock_s3
    def test_create_partitions_dry_run(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()
        cli = Cli()

        partitions = self.helper.create_many_partitions(count=10)
        partitions.sort()

        expected_output = f"Running Partitioner for {self.database}.{self.table}\n\tLooking for partitions in s3://{self.bucket}/{self.table}/\n\tFound 10 new partitions to create\n\t"
        expected_output += ", ".join(map(str, partitions))

        out, err = self.get_cmd_output(cli, ["create-partitions", self.database, self.table, "--dry-run"])
        out.should.equal(expected_output)

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        found = partitioner.existing_partitions()
        found.should.have.length_of(0)

    @mock_glue
    @mock_s3
    def test_create_partitions_nothing_new(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()
        cli = Cli()

        partitions = self.helper.create_many_partitions(count=10)
        partitions.sort()
        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        partitioner.create_partitions(partitions)

        expected_output = f"Running Partitioner for {self.database}.{self.table}\n\tLooking for partitions in s3://{self.bucket}/{self.table}/\n\tFound 0 new partitions to create"

        out, err = self.get_cmd_output(cli, ["create-partitions", self.database, self.table])
        out.should.equal(expected_output)

    @mock_glue
    @mock_s3
    def test_create_partitions_error_output(self):
        """ Technically this should _never_ happen, but on the off chance that
        batch_get_partition ever returns bad values we'll leave it in"""
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()
        cli = Cli()

        partitions = self.helper.create_many_partitions(count=10)
        partitions.sort()

        expected_output = f"Running Partitioner for {self.database}.{self.table}\n\tLooking for partitions in s3://{self.bucket}/{self.table}/\n\tFound 10 new partitions to create\n\t"
        expected_output += ", ".join(map(str, partitions))
        expected_output += f"\nOne or more errors occurred when attempting to create partitions\nError on {partitions[0].values}: AlreadyExistsException"

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        partitioner.create_partitions([partitions[0]])
        mock = MagicMock(return_value=partitions)
        partitioner.partitions_to_create = mock

        partitioner_mock = MagicMock(return_value=partitioner)
        cli.get_partitioner = partitioner_mock

        out, err = self.get_cmd_output(cli, ["create-partitions", self.database, self.table])
        out.should.equal(expected_output)
        self.exit_mock.assert_called_with(1)

        fresh_partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        exists = fresh_partitioner.existing_partitions()

        set(exists).should.equal(set(partitions))

    @mock_glue
    @mock_s3
    def test_create_partitions_limit_days(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()
        cli = Cli()

        today = pendulum.now()

        partitions = []
        for i in range(1, 11):
            partition_date = today.subtract(days=i)
            year = partition_date.strftime("%Y")
            month = partition_date.strftime("%m")
            day = partition_date.strftime("%d")
            hour = "03"

            partition = Partition([year, month, day, hour], f"s3://{self.bucket}/{self.table}/{year}/{month}/{day}/{hour}/")
            self.helper.write_partition_to_s3(partition)
            partitions.append(partition)

        partitions.sort()

        expected_output = f"Running Partitioner for {self.database}.{self.table}\n\tLooking for partitions in s3://{self.bucket}/{self.table}/\n\tFound 7 new partitions to create\n\t"
        expected_output += ", ".join(map(str, partitions[3:]))

        out, err = self.get_cmd_output(cli, ["create-partitions", self.database, self.table, "--limit-days=7"])
        out.should.equal(expected_output)

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        found = partitioner.existing_partitions()
        found.should.have.length_of(7)
        set(found).should.equal(set(partitions[3:]))

    @mock_glue
    @mock_s3
    def test_delete_all_partitions_no_partitions(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()
        cli = Cli()

        # case without any partitions
        out, err = self.get_cmd_output(cli, ["delete-all-partitions", self.database, self.table])
        out.should.equal("No partitions found in table test_table")

    @mock_glue
    @mock_s3
    def test_delete_all_partitions_dry_run(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()
        cli = Cli()

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

    @mock_glue
    @mock_s3
    def test_delete_all_partitions(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()
        cli = Cli()

        partitions = self.helper.create_many_partitions(count=10)
        partitions.sort()
        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        partitioner.create_partitions(partitions)

        expected_out = "Deleting the following partitions:"
        for partition in partitions:
            expected_out += f"\n\t{str(partition)}"

        out, err = self.get_cmd_output(cli, ["delete-all-partitions", self.database, self.table])
        out.should.equal(expected_out)

        found_partitions = partitioner.existing_partitions()
        found_partitions.should.have.length_of(0)

    @mock_glue
    @mock_s3
    def test_delete_all_partitions_error(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()
        cli = Cli()

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)

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
    def test_delete_bad_partitions_no_partitions(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()
        cli = Cli()

        out, err = self.get_cmd_output(cli, ["delete-bad-partitions", self.database, self.table])
        out.should.equal("Found 0 partitions to delete")

    @mock_glue
    @mock_s3
    def test_delete_bad_partitions_dry_run(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()
        cli = Cli()

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

    @mock_glue
    @mock_s3
    def test_delete_bad_partitions(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()
        cli = Cli()

        partitions = self.helper.create_many_partitions(count=10, prefix="not-this-table")
        partitions.sort()

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        partitioner.create_partitions(partitions)

        expected_out = "Found 10 partitions to delete\nDeleting the following partitions:"
        for partition in partitions:
            expected_out += f"\n\t{str(partition)}"

        out, err = self.get_cmd_output(cli, ["delete-bad-partitions", self.database, self.table])
        out.should.equal(expected_out)

        found_partitions = partitioner.existing_partitions()
        found_partitions.should.have.length_of(0)

    @mock_glue
    @mock_s3
    def test_delete_bad_partitions_error_output(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()
        cli = Cli()

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
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

        self.exit_mock.assert_called_with(1)

    @mock_glue
    def test_delete_bad_tables_nothing_to_delete(self):
        database_input = self.helper.create_database_input()
        self.glue.create_database(**database_input)
        cli = Cli()

        location = "s3://bucket/root-table/"
        root_table_input = self.helper.create_table_input(location=location)
        self.glue.create_table(**root_table_input)

        out, err = self.get_cmd_output(cli, ["delete-bad-tables", self.database])
        out.should.equal("Nothing to delete")

    @mock_glue
    def test_delete_bad_tables_dry_run(self):
        database_input = self.helper.create_database_input()
        self.glue.create_database(**database_input)
        cli = Cli()

        location = "s3://bucket/root-table/"
        root_table_input = self.helper.create_table_input(location=location)
        self.glue.create_table(**root_table_input)

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

    @mock_glue
    def test_delete_bad_tables(self):
        database_input = self.helper.create_database_input()
        self.glue.create_database(**database_input)
        cli = Cli()

        location = "s3://bucket/root-table/"
        root_table_input = self.helper.create_table_input(location=location)
        self.glue.create_table(**root_table_input)

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

        out, err = self.get_cmd_output(cli, ["delete-bad-tables", self.database])
        out.should.equal(expected_output)

        cleaner = DatabaseCleaner(self.database, aws_region=self.region)
        found_tables = cleaner.child_tables()
        found_tables.should.have.length_of(0)

    @mock_glue
    def test_delete_bad_tables_error_output(self):
        database_input = self.helper.create_database_input()
        self.glue.create_database(**database_input)
        cli = Cli()

        location = "s3://bucket/root-table/"
        root_table_input = self.helper.create_table_input(location=location)
        self.glue.create_table(**root_table_input)

        table_input = self.helper.create_table_input(location=location, name="test_table-bazer")
        self.glue.create_table(**table_input)

        mock = MagicMock()
        mock.return_value = [{
            "TableName": "test_table-bazer",
            "ErrorDetail": {
                "ErrorCode": "EntityNotFoundException",
                "ErrorMessage": "Table not found",
            },
        }]
        cleaner = DatabaseCleaner(self.database, aws_region=self.region)
        cleaner.delete_tables = mock
        cleaner_mock = MagicMock(return_value=cleaner)
        cli.get_database_cleaner = cleaner_mock

        expected_output = f"Going to delete the following tables:\n\t<Table {self.database} / test_table-bazer : {location}>\nOne or more errors occurred when attempting to delete tables\nError on test_table-bazer: EntityNotFoundException"
        out, err = self.get_cmd_output(cli, ["delete-bad-tables", self.database])
        mock.assert_called()
        out.should.equal(expected_output)

        self.exit_mock.assert_called_with(1)

    @mock_glue
    @mock_s3
    def test_delete_missing_partitions_no_partitions(self):
        self.helper.make_database_and_table()
        cli = Cli()

        self.s3.create_bucket(Bucket=self.bucket)
        partitions = self.helper.create_many_partitions(count=10)
        partitions.sort()

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        partitioner.create_partitions(partitions)

        out, err = self.get_cmd_output(cli, ["delete-missing-partitions", self.database, self.table])
        out.should.equal("Found 0 partitions to delete:")

        catalog_partitions = partitioner.existing_partitions()
        catalog_partitions.should.have.length_of(10)

    @mock_glue
    @mock_s3
    def test_delete_missing_partitions_dry_run(self):
        self.helper.make_database_and_table()
        cli = Cli()

        self.s3.create_bucket(Bucket=self.bucket)
        partitions = self.helper.create_many_partitions(count=10)
        partitions.sort()

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        partitioner.create_partitions(partitions)

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

        s3resource = boto3.resource("s3")
        bucket = s3resource.Bucket(self.bucket)
        for obj in bucket.objects.all():
            obj.delete()

        expected_out = "Found 10 partitions to delete:"
        for partition in partitions:
            expected_out += f"\n\t{partition}"

        out, err = self.get_cmd_output(cli, ["delete-missing-partitions", self.database, self.table])
        out.should.equal(expected_out)

        found_partitions = partitioner.existing_partitions()
        found_partitions.should.have.length_of(0)

    @mock_glue
    @mock_s3
    def test_delete_missing_partitions_error_output(self):
        self.helper.make_database_and_table()
        cli = Cli()
        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        self.s3.create_bucket(Bucket=self.bucket)

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

        self.exit_mock.assert_called_with(1)

    @mock_s3
    @mock_glue
    def test_update_partitions_no_partitions(self):
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

    @mock_s3
    @mock_glue
    def test_update_partitions_dry_run(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()
        cli = Cli()

        partitions = self.helper.create_many_partitions(10)
        partitions.sort()
        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        partitioner.create_partitions(partitions)

        expected_output = "Found 5 moved partitions"
        partitions_to_move = partitions[0:5]
        for p in partitions_to_move:
            subpath = "/".join(p.values)
            new_location = f"s3://old-bucket/old-table/{subpath}/"
            p.location = new_location
            expected_output += f"\n\t{p}"

        partitioner.update_partition_locations(partitions_to_move)

        out, err = self.get_cmd_output(cli, ["update-partitions", self.database, self.table, "--dry-run"])
        out.should.equal(expected_output)

        found_map = PartitionMap(partitioner.existing_partitions())
        for partition in partitions_to_move:
            matching = found_map.get(partition)
            matching.should_not.be.false
            matching.location.startswith(f"s3://{self.bucket}/{self.table}/").should.be.false

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

        expected_output = "Found 5 moved partitions"
        partitions_to_move = partitions[0:5]
        for p in partitions_to_move:
            subpath = "/".join(p.values)
            new_location = f"s3://old-bucket/old-table/{subpath}/"
            p.location = new_location
            expected_output += f"\n\t{p}"

        partitioner.update_partition_locations(partitions_to_move)

        out, err = self.get_cmd_output(cli, ["update-partitions", self.database, self.table])
        out.should.equal(expected_output)

        found_map = PartitionMap(partitioner.existing_partitions())
        for partition in partitions_to_move:
            matching = found_map.get(partition)
            matching.should_not.be.false
            matching.location.startswith(f"s3://{self.bucket}/{self.table}/").should.be.true

    @mock_s3
    @mock_glue
    def test_update_partitions_error_output(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()
        cli = Cli()

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)

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

        self.exit_mock.assert_called_with(1)

    @mock_s3
    @mock_glue
    def test_get_partitioner_and_cleaner_exceptions(self):
        Args = namedtuple("Args", ["database", "table", "profile"])
        args = Args("nodb", "notable", "noprofile")

        no_profile_mock = MagicMock()
        no_profile_mock.side_effect = GlutilError(
            error_type="ProfileNotFound",
            message="No such profile noprofile")
        original_init = Partitioner.__init__
        Partitioner.__init__ = no_profile_mock
        cli = Cli()

        try:
            with captured_output() as (out, err):
                cli.get_partitioner(args)
            output = out.getvalue().strip()
            output.should.equal("No such profile noprofile\n\tConfirm that noprofile is a locally configured aws profile.")
            self.exit_mock.assert_called_with(1)

            no_access_mock = MagicMock()
            no_access_mock.side_effect = GlutilError(
                error_type="AccessDenied",
                message="You do not have permissions to run GetTable")
            Partitioner.__init__ = no_access_mock

            with captured_output() as (out, err):
                cli.get_partitioner(args)
            output = out.getvalue().strip()
            output.should.equal("You do not have permissions to run GetTable\n\tConfirm that noprofile has the glue:GetTable permission.")
            self.exit_mock.assert_called_with(1)

            with captured_output() as (out, err):
                cli.get_partitioner(Args("nodb", "notable", None))
            output = out.getvalue().strip()
            output.should.equal("You do not have permissions to run GetTable\n\tDid you mean to run this with a profile specified?")
            self.exit_mock.assert_called_with(1)

            not_found_mock = MagicMock()
            not_found_mock.side_effect = GlutilError(
                error_type="EntityNotFound",
                message="Error, could not find table notable")
            Partitioner.__init__ = not_found_mock

            with captured_output() as (out, err):
                cli.get_partitioner(args)
            output = out.getvalue().strip()
            output.should.equal("Error, could not find table notable\n\tConfirm notable exists, and you have the ability to access it.")
            self.exit_mock.assert_called_with(1)
        finally:
            # NOTE: this must stay, otherwise tests run after this will still
            #       have Partitioner.__init__ set to a mock
            Partitioner.__init__ = original_init
