from unittest import TestCase
from unittest.mock import MagicMock, call
from moto import mock_glue
from faker import Faker
import boto3
import random
import string
import sure

from glutil import DatabaseCleaner
from glutil.database_cleaner import Table


class DatabaseCleanerTestHelper(object):
    def __init__(self):
        self.faker = Faker()

    def create_table_input(
            self,
            database="test_database",
            name=None,
            location=None):
        if not name:
            to = random.randrange(5, 15)
            name = "".join(random.choice(string.ascii_lowercase)
                           for i in range(0, to))

        if not location:
            path = self.faker.uri_path()
            location = "s3://test-bucket/{}".format(path)

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
                )))

    def create_database_input(self, database_name="test_database"):
        return dict(DatabaseInput=dict(
            Name=database_name,
            Description="You know, for testing"))


class DatabaseCleanerTest(TestCase):
    database = "test_database"

    def setUp(self):
        super().setUp()
        self.helper = DatabaseCleanerTestHelper()

    @mock_glue
    def test_table_trees(self):
        client = boto3.client("glue")
        database_input = self.helper.create_database_input()
        client.create_database(**database_input)

        table1_input = self.helper.create_table_input(
            location="s3://test-bucket/table/")
        client.create_table(**table1_input)

        table2_input = self.helper.create_table_input(
            location="s3://test-bucket/table/child/")
        client.create_table(**table2_input)

        cleaner = DatabaseCleaner("test_database")

        trees = cleaner.table_trees
        tree = trees["test-bucket"]

        child_tables = tree.child_tables()
        child_tables.should.have.length_of(2)
        child_tables[0].location.should.equal("s3://test-bucket/table/")
        child_tables[1].location.should.equal("s3://test-bucket/table/child/")

        first_node = tree.children["table"]
        first_node.tables.should.have.length_of(1)
        first_node.tables[0].location.should.equal("s3://test-bucket/table/")

        table_child_tables = first_node.child_tables()
        table_child_tables.should.have.length_of(1)
        table_child_tables[0].location.should.equal(
            "s3://test-bucket/table/child/")

        child_node = first_node.children["child"]
        child_node.tables.should.have.length_of(1)
        child_node.tables[0].location.should.equal(
            "s3://test-bucket/table/child/")

        child_child_tables = child_node.child_tables()
        child_child_tables.should.be.empty

    @mock_glue
    def test_child_tables(self):
        client = boto3.client("glue")
        database_input = self.helper.create_database_input()
        client.create_database(**database_input)

        table1_input = self.helper.create_table_input(
            location="s3://test-bucket/table/")
        client.create_table(**table1_input)

        table2_input = self.helper.create_table_input(
            location="s3://test-bucket/table/child/")
        client.create_table(**table2_input)

        cleaner = DatabaseCleaner("test_database")

        child_tables = cleaner.child_tables()
        child_tables.should.have.length_of(1)
        child_tables[0].location.should.equal("s3://test-bucket/table/child/")

    @mock_glue
    def test_child_tables_same_location(self):
        client = boto3.client("glue")
        database_input = self.helper.create_database_input()
        client.create_database(**database_input)

        table1_input = self.helper.create_table_input(
            name="table", location="s3://test-bucket/table/")
        client.create_table(**table1_input)

        table2_input = self.helper.create_table_input(
            name="table-foobarbaz", location="s3://test-bucket/table/")
        client.create_table(**table2_input)

        cleaner = DatabaseCleaner("test_database")

        print(cleaner.table_trees)

        child_tables = cleaner.child_tables()
        child_tables.should.have.length_of(1)
        child_tables[0].location.should.equal("s3://test-bucket/table/")
        child_tables[0].name.should.equal("table-foobarbaz")

    @mock_glue
    def test_delete_tables(self):
        client = boto3.client("glue")
        database_input = self.helper.create_database_input()
        client.create_database(**database_input)

        table1_input = self.helper.create_table_input(
            name="table", location="s3://test-bucket/table/")
        client.create_table(**table1_input)
        table1_input["TableInput"]["DatabaseName"] = table1_input["DatabaseName"]
        table1 = Table(table1_input["TableInput"])

        table2_input = self.helper.create_table_input(
            name="table-foobarbaz", location="s3://test-bucket/table/")
        client.create_table(**table2_input)
        table2_input["TableInput"]["DatabaseName"] = table2_input["DatabaseName"]
        table2 = Table(table2_input["TableInput"])

        cleaner = DatabaseCleaner("test_database")

        result = cleaner.delete_tables([table1, table2])
        result.should.be.empty
