from unittest import TestCase
from unittest.mock import MagicMock
from moto import mock_glue, mock_s3
from .helper import GlueHelper, captured_output
import boto3
import sure  # noqa: F401
import sys

from glutil.serverless_function import create_found_partitions, handle
from glutil import Partitioner

class LambdaTest(TestCase):
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

    @mock_glue
    @mock_s3
    def test_create_partitions(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        partitions = self.helper.create_many_partitions(count=10)
        partitions.sort()

        expected_output = f"Running Partitioner for {self.database}.{self.table}\n\tLooking for partitions in s3://{self.bucket}/{self.table}/\n\tFound 10 new partitions to create\n\t"
        expected_output += ", ".join(map(str, partitions))

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        with captured_output() as (out, err):
            create_found_partitions(partitioner, dry_run=False)
        output = out.getvalue().strip()
        output.should.equal(expected_output)

        found = partitioner.partitions_on_disk()
        set(found).should.equal(set(partitions))

    @mock_glue
    @mock_s3
    def test_create_partitions_dry_run(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        partitions = self.helper.create_many_partitions(count=10)
        partitions.sort()

        expected_output = f"Running Partitioner for {self.database}.{self.table}\n\tLooking for partitions in s3://{self.bucket}/{self.table}/\n\tFound 10 new partitions to create\n\t"
        expected_output += ", ".join(map(str, partitions))

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        with captured_output() as (out, err):
            create_found_partitions(partitioner, dry_run=True)
        output = out.getvalue().strip()
        output.should.equal(expected_output)

        found = partitioner.existing_partitions()
        found.should.have.length_of(0)

    @mock_glue
    @mock_s3
    def test_create_partitions_nothing_new(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        partitions = self.helper.create_many_partitions(count=10)
        partitions.sort()
        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        partitioner.create_partitions(partitions)

        expected_output = f"Running Partitioner for {self.database}.{self.table}\n\tLooking for partitions in s3://{self.bucket}/{self.table}/\n\tFound 0 new partitions to create"

        with captured_output() as (out, err):
            create_found_partitions(partitioner)
        output = out.getvalue().strip()
        output.should.equal(expected_output)

    @mock_glue
    @mock_s3
    def test_create_partitions_error_output(self):
        self.s3.create_bucket(Bucket=self.bucket)
        self.helper.make_database_and_table()

        partitions = self.helper.create_many_partitions(count=10)
        partitions.sort()

        expected_output = f"Running Partitioner for {self.database}.{self.table}\n\tLooking for partitions in s3://{self.bucket}/{self.table}/\n\tFound 10 new partitions to create\n\t"
        expected_output += ", ".join(map(str, partitions))
        expected_output += f"\nOne or more errors occurred when attempting to create partitions\nError on {partitions[0].values}: AlreadyExistsException"

        partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        partitioner.create_partitions([partitions[0]])
        mock = MagicMock(return_value=[])
        partitioner.existing_partitions = mock

        with captured_output() as (out, err):
            create_found_partitions(partitioner)
        output = out.getvalue().strip()
        output.should.equal(expected_output)
        self.exit_mock.assert_called_with(1)

        fresh_partitioner = Partitioner(self.database, self.table, aws_region=self.region)
        exists = fresh_partitioner.existing_partitions()

        set(exists).should.equal(set(partitions))

    def test_handle(self):
        import glutil.serverless_function

        original_fn = glutil.serverless_function.create_found_partitions
        original_init = Partitioner.__init__
        init_mock = MagicMock(return_value=None)
        Partitioner.__init__ = init_mock
        fn_mock = MagicMock()
        glutil.serverless_function.create_found_partitions = fn_mock

        try:
            handle({"database": self.database, "table": self.table}, None)
            init_mock.assert_called_with(self.database, self.table, aws_profile=None, aws_region=None)
            fn_mock.assert_called()

            handle({"database": self.database, "table": self.table, "region": self.region}, None)
            init_mock.assert_called_with(self.database, self.table, aws_profile=None, aws_region=self.region)
            fn_mock.assert_called()
        finally:
            Partitioner.__init__ = original_init
            glutil.serverless_function.create_found_partitions = original_fn
