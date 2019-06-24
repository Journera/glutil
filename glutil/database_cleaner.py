import boto3
from functools import total_ordering
from .utils import paginated_response, GlutilError


@total_ordering
class Table(object):
    def __init__(self, raw):
        self.raw = raw
        self.name = raw["Name"]
        self.location = raw["StorageDescriptor"]["Location"]
        if self.location[-1] != "/":
            self.location += "/"

        self.database = raw["DatabaseName"]

        full_path = self.location[len("s3://"):]
        segments = full_path.split("/")
        self.bucket = segments[0]
        self.path = "/".join(segments[1:])

    def __repr__(self):  # pragma: no cover
        return f"<Table {self.database} / {self.name} : {self.location}>"

    def __eq__(self, other):
        return self.database == other.database and \
            self.location == other.location and \
            self.name == other.name

    def __lt__(self, other):
        if self.database != other.database:
            return self.database > other.database
        elif self.location != other.location:
            return self.location > other.location
        return self.name > other.name

    def __hash__(self):
        hash_string = repr(self)
        return hash(hash_string)


class TableTree(object):
    """TableTree is a way of representing tables in S3.

    TableTree represents a mapping of S3 paths to tables. Each TableTree
    is a node mapped to an S3 path, with two additional pieces of information:
    all directories below the current path, and the Glue tables whose locations
    map are the same as the current path.

    In dict form, a TableTree looks like this:

        {
            "bucket": "s3-bucket-name",
            "path": "path/in/bucket",
            "tables": [
                Table, Table, Table
            ],
            "children": {
                "child": {
                    "bucket": s3-bucket-name",
                    "path": "path/in/bucket/child",
                    "tables": [Table],
                    "children": { ... },
                },
                "child2": {"path": "path/in/bucket/child2"},
                ...
            }
        }

    In general practice, a TableTree is not incredibly useful, as Glue
    tables should own everything below their defined location. However,
    sometimes a Glue Crawler goes bad and creates tables which should've been
    partitions of an existing table. In those cases TableTree is useful
    because it allows you to find all of the erroneously created tables.
    """

    @classmethod
    def from_table(cls, table):
        tree = cls(table.bucket, table.path)
        tree.add_table(table)
        return tree

    def __init__(self, bucket, path):
        self.bucket = bucket

        if path and path[-1] != "/":
            path = path + "/"

        self.path = path
        self.tables = []
        self.children = {}

    def __repr__(self):  # pragma: no cover
        return f"<TableTree: {self.bucket} / {self.path}, {len(self.tables)} table(s)>"

    def add_table(self, table):
        if table.bucket != self.bucket:
            raise ValueError(f"add_table() can only add tables in the same bucket. tree is '{self.bucket}'. table is '{table.bucket}'.")

        path = table.path

        if self.path == path:
            self.tables.append(table)
            return

        if not path.startswith(self.path):
            raise ValueError(f"add_table() can only add tables in the same tree. tree is '{self.path}'. table is '{path}'.")

        rest = path[len(self.path):]
        # if rest[0] == "/":
        #     rest = rest[1:]

        segments = rest.split("/")
        next_segment = segments[0]

        # does it exist?
        if next_segment not in self.children:
            next_path = self.path
            next_path += next_segment

            next_map = TableTree(self.bucket, next_path)
            self.children[next_segment] = next_map

        self.children[next_segment].add_table(table)

    def child_tables(self):
        tables = []
        for _, node in self.children.items():
            tables.extend(node.tables)
            tables.extend(node.child_tables())

        return tables

    def walk(self, fn):
        fn(self)
        for _, node in self.children.items():
            node.walk(fn)


class DatabaseCleaner(object):
    """DatabaseCleaner is used to clean up a database in the Glue
    Data Catalog after a Crawler creates tables which it should've
    picked up as partitions.
    """

    def __init__(self, database, aws_profile=None, aws_region=None):
        self.database = database
        self.session = boto3.Session(
            profile_name=aws_profile,
            region_name=aws_region)
        self.glue = self.session.client("glue")

    def refresh_trees(self):
        self._table_trees = self._get_table_trees()

    @property
    def table_trees(self):
        if not hasattr(self, "_table_trees"):
            self.refresh_trees()
        return self._table_trees

    def _get_table_trees(self):
        """table_trees generates a dict of s3 bucket names to TableTree"""

        try:
            all_tables = map(
                Table, paginated_response(
                    self.glue.get_tables,
                    {"DatabaseName": self.database},
                    "TableList",
                ))
        except self.glue.exceptions.EntityNotFoundException as e:  # pragma: no cover
            raise GlutilError(
                error_type="EntityNotFound",
                message="No `{}` table found".format(self.database),
                source=e)
        except self.glue.exceptions.AccessDeniedException as e:  # pragma: no cover
            raise GlutilError(
                error_type="AccessDenied",
                message="You do not have permissions to call glue:GetTables",
                source=e)

        s3_tables = filter(
            lambda t: t.location.startswith("s3"),
            all_tables)

        trees = {}
        for table in s3_tables:
            if table.bucket not in trees:
                trees[table.bucket] = TableTree(table.bucket, "")

            trees[table.bucket].add_table(table)

        return trees

    def child_tables(self):
        """Get all tables that exist below another table"""

        child_tables = []

        def find_children(node):
            num_tables = len(node.tables)

            # if the current node has no tables, skip
            if num_tables == 0:
                return

            # add all children from current node
            if num_tables > 0:
                child_tables.extend(node.child_tables())

            # if there's more than one table in this node, determine which is
            # the "real" table - glue makes this easy by appending a unique
            # identifier on all tables created in a path with a matching name
            # (ex, if you have a table called "foo", and the glue crawler
            # decided a table exists in a directory called "foo", it'll name
            # the table something like "foo-somelongstring")
            if num_tables > 1:
                for idx, table in enumerate(node.tables):
                    for j in range(idx + 1, num_tables):
                        other = node.tables[j]
                        if table.name.startswith(other.name):
                            child_tables.append(table)
                        elif other.name.startswith(table.name):
                            child_tables.append(other)

        for node in self.table_trees.values():
            node.walk(find_children)

        child_tables = list(set(child_tables))
        child_tables.sort(key=lambda t: t.name)

        return child_tables

    def delete_tables(self, tables_to_delete):
        response = self.glue.batch_delete_table(
            DatabaseName=self.database,
            TablesToDelete=list(map(lambda t: t.name, tables_to_delete)))

        if "Errors" in response:
            return response["Errors"]

        return []
