import re
import boto3
import botocore
import datetime
from functools import total_ordering
from .utils import grouper, GlutilError, paginated_response


@total_ordering
class Partition(object):
    @classmethod
    def from_aws_response(cls, response):
        partitions = response["Values"]
        location = response["StorageDescriptor"]["Location"]

        if location[-1] != "/":
            location += "/"

        return cls(partitions, location, raw=response)

    def __init__(self, partitions, location, raw=None):
        self.partitions = list(partitions)
        self.location = location
        self.raw = raw

    def __str__(self):
        joined_values = ", ".join(self.partitions)
        return f"[{joined_values}]"

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
        return self.partitions


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
            num_partitions = len(p.partitions)
            current_d = d

            for i in range(0, num_partitions - 1):
                this_part = p.partitions[i]

                if this_part not in current_d:
                    current_d[this_part] = {}

                current_d = current_d[this_part]

            current_d[p.partitions[-1]] = p

        return d

    def __init__(self, partitions):
        self.map = self.partitions_to_map(partitions)

    def get(self, partition):
        num_partitions = len(partition.partitions)
        current_map = self.map

        for i in range(0, num_partitions - 1):
            this_part = partition.partitions[i]
            current_map = current_map.get(this_part, {})

        return current_map.get(partition.partitions[-1], None)


class Partitioner(object):
    PARTITION_MATCH = r"(year=|)(?P<year>\d{4})/(month=|)(?P<month>\d{2})/(day=|)(?P<day>\d{2})/(hour=|)(?P<hour>\d{2})/"

    def __init__(self, database, table, aws_profile=None, aws_region=None):
        self.database = database
        self.table = table

        try:
            self.session = boto3.Session(
                profile_name=aws_profile,
                region_name=aws_region)
        except botocore.exceptions.ProfileNotFound as e:  # pragma: no cover
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
        self.partition_keys = self.table_definition["Table"]["PartitionKeys"]

        self.bucket, self.prefix = self._get_bucket()

        if self.prefix != "" and self.prefix[-1] != "/":
            self.prefix += "/"

    def _get_bucket(self):
        s3_arn = self.table_definition['Table']['StorageDescriptor']['Location']

        s3_arn_split = s3_arn.split('/')
        bucket_name = s3_arn_split[2]
        prefix = "/".join(s3_arn_split[3:])

        return bucket_name, prefix

    def partitions_on_disk(self, limit_days=0):
        """Find partitions in S3.

        This function will crawl S3 for any partitions it can find.

        And will return a :obj:`list` of :obj:`Partition` matching those found
        paths.

        Args:
            limit_days (`int`): Providing a value other than 0 will limit the
                search to only partitions created in the past N days.

                NOTE: limit_days only works if your first three partition keys are
                [year, month, day]. Any other first three partitions will raise
                an exception if limit_days is non-zero.
        """

        if not isinstance(limit_days, int) or limit_days < 0:
            raise ValueError("invalid value for limit_days, must be an integer of >=0")

        if limit_days == 0:
            return self._all_partitions_on_disk()

        # only year/month/keys, these are the partitions
        partition_keys = [k["Name"].lower() for k in self.partition_keys[:3]]
        if partition_keys != ["year", "month", "day"]:
            raise TypeError("limit_days only works on tables partitioned by year, month, and day")

        # determine all possible path prefixes for days
        partition_prefixes = []
        today = datetime.datetime.now()
        for i in range(0, limit_days + 1):
            date_delta = datetime.timedelta(days=i)
            partition_date = today - date_delta
            values = [
                str(partition_date.year),
                str(partition_date.month),
                str(partition_date.day),
            ]

            hive_format = partition_date.strftime(f"{self.prefix}year=%Y/month=%m/day=%d/")
            flat_format = partition_date.strftime(f"{self.prefix}%Y/%m/%d/")
            partition_prefixes.append({"prefix": hive_format, "values": values})
            partition_prefixes.append({"prefix": flat_format, "values": values})

        partitions = []
        if len(self.partition_keys) == 3:
            for prefix in partition_prefixes:
                partition = self._confirm_partition(**prefix)
                if partition:
                    partitions.append(partition)
        else:
            for prefix in partition_prefixes:
                partitions.extend(self._partition_finder(prefix["prefix"], idx=3, values=prefix["values"]))

        return partitions

    def _partition_finder(self, prefix, idx=0, values=[]):
        strict = len(self.partition_keys) == 1

        this_key = self.partition_keys[idx]
        regex = r".*"
        if this_key["Type"] == "int":
            regex = r"\d{1,}"
        key = this_key["Name"]

        last_key = idx == len(self.partition_keys) - 1
        partitions = []
        for match in self._prefix_match(prefix, key, regex, strict=strict):
            these_values = values.copy()
            these_values.append(match["value"])
            if last_key:
                this_prefix = match["prefix"]
                partitions.append(Partition(
                    these_values,
                    f"s3://{self.bucket}/{this_prefix}",
                ))
            else:
                partitions.extend(self._partition_finder(match["prefix"], idx=idx + 1, values=these_values))

        return partitions

    def _all_partitions_on_disk(self):
        partitions = self._partition_finder(self.prefix)

        if not partitions and len(self.partition_keys) == 1:
            flat_partitions = self._flat_partitions_on_disk()
            partitions.extend(flat_partitions)

        return partitions

    def _flat_partitions_on_disk(self):
        resp = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix)

        items = set()

        prefix_len = len(self.prefix)
        for obj in resp["Contents"]:
            name = obj["Key"][prefix_len:]
            splits = name.split("/")
            location_suffix = "/".join(splits[:-1])
            partition_key = "-".join(splits[:-1])

            partition = Partition(
                [partition_key],
                f"s3://{self.bucket}/{self.prefix}{location_suffix}/")
            items.add(partition)

        return list(items)

    def _prefix_match(self, prefix, partition_name, partition_value_regex, strict=False):
        base_regex = "({}=|)({})/"
        if strict:
            base_regex = "({}=)({})/"
        regex = base_regex.format(partition_name, partition_value_regex)

        resp = self.s3.list_objects_v2(
            Bucket=self.bucket, Delimiter="/", Prefix=prefix)
        if "CommonPrefixes" not in resp:
            return []

        items = []

        prefix_len = len(prefix)
        for obj in resp["CommonPrefixes"]:
            name = obj["Prefix"][prefix_len:]
            match = re.search(regex, name)
            if match:
                items.append({"prefix": obj["Prefix"], "value": match.group(2)})

        return items

    def _confirm_partition(self, prefix, values):
        resp = self.s3.list_objects_v2(
            Bucket=self.bucket, Delimiter="/", Prefix=prefix)
        if resp["KeyCount"] == 0:
            return None
        return Partition(values, f"s3://{self.bucket}/{prefix}")

    def existing_partitions(self):
        args = {
            "DatabaseName": self.database,
            "TableName": self.table,
        }
        raw_partitions = paginated_response(
            self.glue.get_partitions, args, "Partitions")

        return sorted([Partition.from_aws_response(p) for p in raw_partitions])

    def partitions_to_create(self, partitions):
        if len(partitions) == 0:
            return []

        found_partitions = []

        # batch_get_partition has a limit of 1000 per call
        groups = grouper(partitions, 1000)

        for group in groups:
            partitions_to_get = []
            for partition in group:
                partitions_to_get.append({"Values": partition.values})

            resp = self.glue.batch_get_partition(
                DatabaseName=self.database,
                TableName=self.table,
                PartitionsToGet=partitions_to_get)

            found_partitions.extend(resp["Partitions"])

        found_partitions_as_partition = [
            Partition.from_aws_response(p) for p in found_partitions
        ]

        return set(partitions) - set(found_partitions_as_partition)

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
            "Values": partition.values,
            "StorageDescriptor": storage_desc,
        }

    def bad_partitions(self):
        """Return a list of bad partitions

        Bad partitions are defined as any partition that exists in the
        Glue Data Catalog, but do not exist on disk, or exist on disk
        in a different location.

        For example, a partition with the values [2019, 01, 01, 01] exists in
        the Glue Data Catalog with the location 2019/02/02/02 is considered bad.
        Similarly, a partition that exists in s3://another-bucket/, while the table's
        location is set to s3://this-bucket/ is also considered bad.

        Returns:
            list of Partition objects
        """
        missing = set(self.missing_partitions())

        existing = set(self.existing_partitions())
        found = set(self.partitions_on_disk())

        to_delete = list((existing - found) | missing)
        to_delete.sort()
        return to_delete

    def missing_partitions(self):
        """Return a list of partitions that exist in the Glue Data Catalog,
        but not in the S3 bucket.
        """

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
        """Remove partitions from the Glue Data Catalog

        Args:
            partitions_to_delete (list<Partition>): A list of Partition objects to remove from
                the Glue Data Catalog

        Returns:
            list of Errors:

                {
                    "Partition": partition,
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
                {"Values": p.values} for p in group]

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
        been updated in the Glue Data Catalog, and that the underlying data has also
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

        This function will use the existing partition definition in the Glue Data
        Catalog and only update the StorageDescriptor.Location in the catalog.
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
