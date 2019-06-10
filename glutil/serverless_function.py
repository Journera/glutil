from .partitioner import Partitioner
from .utils import grouper, print_batch_errors
import sys

def handle(event, context):
    database = event["database"]
    table = event["table"]

    if "profile" in event:
        profile = event["profile"]
    else:
        profile = None

    partitioner = Partitioner(database, table, profile)

    print(f"Running Partitioner for {database}.{table}")
    print(f"\tLooking for partitions in s3://{partitioner.bucket}/{partitioner.prefix}")

    found_partitions = set(partitioner.partitions_on_disk())
    existing_partitions = set(partitioner.existing_partitions())
    to_create = sorted(found_partitions - existing_partitions)

    print(f"\tFound {len(to_create)} new partitions to create")

    # break early
    if len(to_create) == 0:
        return

    if len(partitions) <= 50:
        print("\t{}".format(", ".join(to_create)))

    errors = partitioner.create_partitions(to_create)
    if errors:
        print_batch_errors(errors, "partition", "PartitionValues")
        sys.exit(1)
