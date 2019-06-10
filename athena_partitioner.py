#!/usr/bin/env python3

from glutil.partitioner import Partitioner
import glutil.serverless_function as function


def handle(event, context):
    function.handle(event, context)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
Find and create Athena/Glue partitions based off files in S3.

The partitioner will search for directory structures below your table's defined
S3 location that conform to a `year/month/day/hour` structure, formatted just
as numbers or in hive-partitioned `year=yyyy/month=mm/day=dd/hour=hh/` format.
        """)
    parser.add_argument(
        "database",
        type=str,
        default="",
        help="The Athena/Glue database containing the table you want to search for partitions")
    parser.add_argument(
        "table",
        type=str,
        help="The Athena/Glue table you want to search for partitions")
    parser.add_argument("--profile", "-p", type=str, help="AWS profile to use")
    args = parser.parse_args()

    handle({
        "database": args.database,
        "table": args.table,
        "profile": args.profile},
        None
    )
