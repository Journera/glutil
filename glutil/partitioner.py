import re
import boto3
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

    def __le__(self, other):
        return self._cmp(other) < 0

    def __ge__(self, other):
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
            .get(partition.hour, None) \



class Partitioner(object):
    def __init__(self, database, table, aws_profile=None, aws_region=None):
        self.database = database
        self.table = table

        self.session = boto3.Session(
            profile_name=aws_profile,
            region_name=aws_region)
        self.s3 = self.session.client("s3")
        self.glue = self.session.client("glue")

        try:
            self.table_definition = self.glue.get_table(
                DatabaseName=self.database,
                Name=self.table)
        except self.glue.exceptions.AccessDeniedException as e:  # pragma: no cover
            raise GlutilError(
                message="You do not have permission to run GetTable",
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

    def create_new_partitions(self):
        print(
            "Running partitioner for {}.{}".format(
                self.database,
                self.table))
        print("\tLooking for partitions in s3://{}/{}".format(self.bucket, self.prefix))

        found_partitions = set(self.partitions_on_disk())
        existing_partitions = set(self.existing_partitions())
        partitions = sorted(found_partitions - existing_partitions)

        print("\tfound {} new partitions to create".format(len(partitions)))

        # break early
        if len(partitions) == 0:
            return

        if len(partitions) <= 50:
            print("\t{}".format(", ".join(map(str, partitions))))

        groups = list(grouper(partitions, 100))
        num_groups = len(groups)

        all_errors = []
        for i, group in enumerate(groups):
            print("\tCreating chunk {} of {}".format(i + 1, num_groups))
            current_partitions = filter(None, group)
            errors = self._create_partitions(current_partitions)
            if errors:
                all_errors.extend(errors)
        return all_errors

    def _create_partitions(self, partitions):
        partition_input = list(map(self._partition_input, partitions))

        response = self.glue.batch_create_partition(
            DatabaseName=self.database,
            TableName=self.table,
            PartitionInputList=partition_input,
        )

        if "Errors" in response:
            return response["Errors"]
        return []

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
        missing = self.missing_partitions()

        existing = set(self.existing_partitions())
        found = set(self.partitions_on_disk())

        return list(existing - found) + missing

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

        # The batch_delete_partitions API method only supports deleting 25
        # partitions per call.
        groups = grouper(partitions_to_delete, 25)

        for group in groups:
            # the grouper function fills the final group with Nones to reach
            # the determined length. We need to remove those Nones
            partitions = filter(None, group)

            request_input = [
                {"Values": [p.year, p.month, p.day, p.hour]}
                for p in partitions]

            response = self.glue.batch_delete_partitions(
                DatabaseName=self.database,
                TableName=self.table,
                PartitionsToDelete=request_input)

            if "Errors" in response:
                errors.extend(response["Errors"])

        return errors

    def update_moved_partitions(self, dry_run=False):
        """Find matched pairs of partitions on disk and partitions in the
        catalog with the same values, and update the catalog to reflect the
        found disk location"""

        existing = self.existing_partitions()
        found = PartitionMap(self.partitions_on_disk())

        not_updated = set(existing)
        for partition in existing:
            matching = found.get(partition)
            if matching:
                not_updated.remove(partition)

                if partition != matching:
                    print("Updating", partition)

                    print(partition, matching)
                    print(partition.location, matching.location)
                    print()

                    if not dry_run:
                        partition.raw["StorageDescriptor"]["Location"] = matching.location
                        for key in [
                            "CreationTime",
                            "DatabaseName",
                                "TableName"]:
                            if key in partition.raw:
                                del partition.raw[key]

                        self.glue.update_partition(
                            DatabaseName=self.database,
                            TableName=self.table,
                            PartitionValueList=partition.raw["Values"],
                            PartitionInput=partition.raw)

        print(len(not_updated), "partition not updated")
