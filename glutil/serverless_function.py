from .partitioner import Partitioner
from .utils import print_batch_errors
import sys


def create_found_partitions(partitioner, limit_days=0, dry_run=False):
    print(f"Running Partitioner for {partitioner.database}.{partitioner.table}")
    print(f"\tLooking for partitions in s3://{partitioner.bucket}/{partitioner.prefix}")

    found_partitions = set(partitioner.partitions_on_disk(limit_days))
    to_create = sorted(partitioner.partitions_to_create(found_partitions))
    print(f"\tFound {len(to_create)} new partitions to create")

    # break early
    if len(to_create) == 0:
        return

    if len(to_create) <= 50:
        print("\t{}".format(", ".join(map(str, to_create))))

    if not dry_run:
        errors = partitioner.create_partitions(to_create)
        if errors:
            print_batch_errors(errors, action="create")
            sys.exit(1)


def handle(event, context):
    database = event["database"]
    table = event["table"]

    if "region" in event:
        region = event["region"]
    else:
        region = None

    if "limit_days" in event:
        limit_days = int(event["limit_days"])
    else:
        limit_days = 0

    partitioner = Partitioner(database, table, aws_profile=None, aws_region=region)
    create_found_partitions(partitioner, limit_days=limit_days)
