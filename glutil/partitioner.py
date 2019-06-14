import re
import boto3
import botocore
from functools import total_ordering
from .utils import grouper, GlutilError, paginated_response


@total_ordering
class Partition(object):
    def __init__(self, year, month, day, hour, location, raw=None):
        self.year = year
        self.month = month
        self.day = day
        self.hour = hour
        self.location = location
        self.raw = raw

    def __str__(self):
        return "[{}, {}, {}, {}]".format(
            self.year, self.month, self.day, self.hour)

    def __repr__(self):
        return f"<Partition {str(self)} / {self.location}>"

    def __eq__(self, other):
        return self.values == other.values and self.location == other.location

    def _cmp(self, other):
        if self.values > other.values:
            return 1
        elif self.values < other.values:
            return -1
        # these are reversed because we want alphabetical ordering,
        # and b > a with python semantics
        elif self.location < other.location:
            return 1
        elif self.location > other.location:
            return -1

        return 0

    def __lt__(self, other):
        return self._cmp(other) < 0

    def __gt__(self, other):
        return self._cmp(other) > 0

    def __hash__(self):
        hash_string = str(self) + self.location
        return hash(hash_string)

    @property
    def values(self):
        return [self.year, self.month, self.day, self.hour]


class PartitionMap(object):
    """PartitionMap creates nested dicts of partitions, using their values as
    the keys.

    For example, if you have a partition with the values [2019, 01, 02, 03]
    it can be found in PartitionMap["2019"]["01"]["02"]["03"].

    This is used when updating partition locations after the underlying data is
    moved. It allows existing partitions to find their new location quickly
    """

    @staticmethod
    def partitions_to_map(partitions):
        d = {}
        for p in set(partitions):
            if p.year not in d:
                d[p.year] = {}
            if p.month not in d[p.year]:
                d[p.year][p.month] = {}
            if p.day not in d[p.year][p.month]:
                d[p.year][p.month][p.day] = {}

            d[p.year][p.month][p.day][p.hour] = p

        return d

    def __init__(self, partitions):
        self.map = self.partitions_to_map(partitions)

    def get(self, partition):
        return self.map.get(partition.year, {}) \
            .get(partition.month, {}) \
            .get(partition.day, {}) \
            .get(partition.hour, None)


class Partitioner(object):
    def __init__(self, database, table, aws_profile=None, aws_region=None):
        self.database = database
        self.table = table

        try:
            self.session = boto3.Session(
                profile_name=aws_profile,
                region_name=aws_region)
        except botocore.exceptions.ProfileNotFound as e:
            raise GlutilError(
                error_type="ProfileNotFound",
                message=f"No such profile {aws_profile}.",
                source=e)

        self.s3 = self.session.client("s3")
        self.glue = self.session.client("glue")

        try:
            self.table_definition = self.glue.get_table(
                DatabaseName=self.database,
                Name=self.table)
        except self.glue.exceptions.AccessDeniedException as e:  # pragma: no cover
            raise GlutilError(
                error_type="AccessDenied",
                message="You do not have permission to run GetTable",
                source=e)
        except self.glue.exceptions.EntityNotFoundException as e:
            entity_message = e.response["Error"]["Message"]
            raise GlutilError(
                error_type="EntityNotFound",
                message=f"Error, could not find {entity_message}",
                source=e)

        self.storage_descriptor = self.table_definition["Table"]["StorageDescriptor"]

        self.bucket, self.prefix = self._get_bucket()

    def _get_bucket(self):
        s3_arn = self.table_definition['Table']['StorageDescriptor']['Location']

        s3_arn_split = s3_arn.split('/')
        bucket_name = s3_arn_split[2]
        prefix = "/".join(s3_arn_split[3:])

        return bucket_name, prefix

    def partitions_on_disk(self):
        if self.prefix != "" and self.prefix[-1] != "/":
            self.prefix += "/"

        key_regex = r"(year=|)(?P<year>\d{4})/(month=|)(?P<month>\d{2})/(day=|)(?P<day>\d{2})/(hour=|)(?P<hour>\d{2})/"
        partitions = []
        for year_match in self._prefix_match(self.prefix, "year", r"\d{4}"):
            for month_match in self._prefix_match(
                    year_match, "month", r"\d{2}"):
                for day_match in self._prefix_match(
                        month_match, "day", r"\d{2}"):
                    for hour_match in self._prefix_match(
                            day_match, "hour", r"\d{2}"):
                        match = re.search(key_regex, hour_match)
                        location = "s3://{}/{}".format(self.bucket, hour_match)
                        partitions.append(Partition(
                            match.group("year"),
                            match.group("month"),
                            match.group("day"),
                            match.group("hour"),
                            location,
                        ))

        return partitions

    def _prefix_match(self, prefix, partition_name, partition_value_regex):
        regex = "({}=|){}/".format(partition_name, partition_value_regex)

        resp = self.s3.list_objects_v2(
            Bucket=self.bucket, Delimiter="/", Prefix=prefix)
        if "CommonPrefixes" not in resp:
            return []

        items = []

        prefix_len = len(prefix)
        for obj in resp["CommonPrefixes"]:
            name = obj["Prefix"][prefix_len:]
            if re.match(regex, name):
                items.append(obj["Prefix"])

        return items

    def existing_partitions(self):
        args = {
            "DatabaseName": self.database,
            "TableName": self.table,
        }
        raw_partitions = paginated_response(
            self.glue.get_partitions, args, "Partitions")

        return sorted([self._parse_partition(p) for p in raw_partitions])

    def _parse_partition(self, partition):
        values = partition["Values"].copy()
        values.append(partition["StorageDescriptor"]["Location"])

        # append trailing slash, so it looks the same as the new partitions
        if values[-1][-1] != "/":
            values[-1] += "/"

        return Partition(*values, raw=partition)

    def create_partitions(self, partitions):
        groups = grouper(partitions, 100)

        errors = []
        for group in groups:
            partition_input = list(map(self._partition_input, group))

            response = self.glue.batch_create_partition(
                DatabaseName=self.database,
                TableName=self.table,
                PartitionInputList=partition_input,
            )
            if "Errors" in response:
                errors.extend(response["Errors"])
        return errors

    def _partition_input(self, partition):
        storage_desc = self.storage_descriptor.copy()
        storage_desc["Location"] = partition.location
        return {
            "Values": [
                partition.year,
                partition.month,
                partition.day,
                partition.hour],
            "StorageDescriptor": storage_desc,
        }

    def bad_partitions(self):
        """Return a list of bad partitions

        Bad partitions are defined as any partition that exists in AWS, but do
        do not exist on disk, or exist on disk in a different location.

        For example, a partition with the values [2019, 01, 01, 01] exists in
        AWS with the location 2019/02/02/02 is considered bad. Similarly, a
        partition that exists in s3://another-bucket/, while the table's
        location is set to s3://this-bucket/ is also considered bad.

        Returns:
            list of Partition
        """
        missing = set(self.missing_partitions())

        existing = set(self.existing_partitions())
        found = set(self.partitions_on_disk())

        to_delete = list((existing - found) | missing)
        to_delete.sort()
        return to_delete

    def missing_partitions(self):
        """Return a list of partitions that exist in the Glue database, but not
        in the S3 bucket."""

        existing = set(self.existing_partitions())
        found = set(self.partitions_on_disk())
        found_values = [p.values for p in found]

        missing = []
        for partition in existing:
            if partition.values not in found_values:
                missing.append(partition)

        missing.sort()
        return missing

    def delete_partitions(self, partitions_to_delete):
        """Remove partitions from the Glue database

        Args:
            partitions_to_delete (list): A list of Partitions to remove from
                the Glue database

        Returns:
            list of Errors:

                {
                    "Partition": Partition,
                    "ErrorDetail": {
                        "ErrorCode": "The type of error encountered",
                        "ErrorMessage": "A longer description of the error",
                    },
                }
        """

        errors = []

        # The batch_delete_partition API method only supports deleting 25
        # partitions per call.
        groups = grouper(partitions_to_delete, 25)
        for group in groups:
            request_input = [
                {"Values": [p.year, p.month, p.day, p.hour]} for p in group]

            response = self.glue.batch_delete_partition(
                DatabaseName=self.database,
                TableName=self.table,
                PartitionsToDelete=request_input)

            if "Errors" in response:
                errors.extend(response["Errors"])

        return errors

    def find_moved_partitions(self):
        """Find partitions that have been moved to the table's new location.

        This function expects that the table's location has already
        been updated in the glue catalog, and that the underlying data has also
        been moved.

        Returns:
            List of Partitions that exist in the updated S3 location, that
            match the values of a partition that exists in the current catalog,
            that also have a different location than existing partition in the
            catalog.
        """

        existing = self.existing_partitions()
        found = PartitionMap(self.partitions_on_disk())

        moved = []
        for partition in existing:
            matching = found.get(partition)
            if matching:
                if partition != matching:
                    moved.append(matching)

        return moved

    def update_partition_locations(self, moved):
        """Update matched partition locations.

        This function will use the existing partition definition in the glue
        catalog and only update the StorageDescriptor.Location in the catalog.
        All other details will remain the same.

        Args:
            partitions (:obj:`list` of :obj:`Partition`): The partitions to be
                updated. The values should match those currently in the
                catalog, but the location should be the updated location.
                The output of find_moved_partitions matches the expected input.

        Returns:
            List of AWS Error objects, in the form of
            {
                "ErrorDetail": {
                    "ErrorCode": "TheErrorName",
                    "ErrorMessage": "short description"
                }
            }
        """

        errors = []
        for partition in moved:
            try:
                resp = self.glue.get_partition(
                    DatabaseName=self.database,
                    TableName=self.table,
                    PartitionValues=partition.values)

                definition = resp["Partition"]

                definition["StorageDescriptor"]["Location"] = partition.location
                for key in ["CreationTime", "DatabaseName", "TableName"]:
                    del definition[key]

                resp = self.glue.update_partition(
                    DatabaseName=self.database,
                    TableName=self.table,
                    PartitionValueList=definition["Values"],
                    PartitionInput=definition)
            except self.glue.exceptions.EntityNotFoundException as e:
                errors.append({
                    "Partition": partition.values,
                    "ErrorDetail": {
                        "ErrorCode": e.response["Error"]["Code"],
                        "ErrorMessage": e.response["Error"]["Message"]
                    }})

        return errors
