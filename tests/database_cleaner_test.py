from unittest import TestCase
from moto import mock_glue
from .helper import GlueHelper
import boto3
import sure  # noqa: F401

from glutil import DatabaseCleaner
from glutil.database_cleaner import Table


class DatabaseCleanerTest(TestCase):
    database = "test_database"
    region = "us-east-1"

    def setUp(self):
        super().setUp()
        self.helper = GlueHelper()

    @mock_glue
    def test_table_trees(self):
        client = boto3.client("glue", region_name=self.region)
        database_input = self.helper.create_database_input()
        client.create_database(**database_input)

        table1_input = self.helper.create_table_input(
            location="s3://test-bucket/table/")
        client.create_table(**table1_input)

        table2_input = self.helper.create_table_input(
            name="child",
            location="s3://test-bucket/table/child/")
        client.create_table(**table2_input)

        cleaner = DatabaseCleaner("test_database", aws_region=self.region)

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
        client = boto3.client("glue", region_name=self.region)
        database_input = self.helper.create_database_input()
        client.create_database(**database_input)

        table1_input = self.helper.create_table_input(
            location="s3://test-bucket/table/")
        client.create_table(**table1_input)

        table2_input = self.helper.create_table_input(
            name="child",
            location="s3://test-bucket/table/child/")
        client.create_table(**table2_input)

        cleaner = DatabaseCleaner("test_database", aws_region=self.region)

        child_tables = cleaner.child_tables()
        child_tables.should.have.length_of(1)
        child_tables[0].location.should.equal("s3://test-bucket/table/child/")

    @mock_glue
    def test_child_tables_same_location(self):
        client = boto3.client("glue", region_name=self.region)
        database_input = self.helper.create_database_input()
        client.create_database(**database_input)

        table1_input = self.helper.create_table_input(
            name="table", location="s3://test-bucket/table/")
        client.create_table(**table1_input)

        table2_input = self.helper.create_table_input(
            name="table-foobarbaz", location="s3://test-bucket/table/")
        client.create_table(**table2_input)

        cleaner = DatabaseCleaner("test_database", aws_region=self.region)

        print(cleaner.table_trees)

        child_tables = cleaner.child_tables()
        child_tables.should.have.length_of(1)
        child_tables[0].location.should.equal("s3://test-bucket/table/")
        child_tables[0].name.should.equal("table-foobarbaz")

    @mock_glue
    def test_delete_tables(self):
        client = boto3.client("glue", region_name=self.region)
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

        cleaner = DatabaseCleaner("test_database", aws_region=self.region)

        result = cleaner.delete_tables([table1, table2])
        result.should.be.empty

class TableTest(TestCase):
    def make_table(self, name, location, database):
        return Table({
            "Name": name,
            "DatabaseName": database,
            "StorageDescriptor": {"Location": location},
        })

    def test_table_comparisons(self):
        t1 = self.make_table("foo", "s3://bucket/foo/", "db1")
        t2 = self.make_table("bar", "s3://bucket/bar/", "db1")
        (t1 > t2).should.be.false
        (t1 < t2).should.be.true

        t3 = self.make_table("groo", "s3://bucket/foo/", "db1")
        (t1 > t3).should.be.true

        t4 = self.make_table("foo", "s3://bucket/z-foo/", "db1")
        (t1 > t4).should.be.true

        t5 = self.make_table("foo", "s3://bucket/foo/", "db1")
        t5.should.equal(t1)

        t6 = self.make_table("foo", "s3://bucket/foo/", "db2")
        (t1 > t6).should.be.true

        t7 = self.make_table("foo", "s3://bucket/foo", "db1")
        t7.should.equal(t1)
