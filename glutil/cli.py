import argparse
import glutil.text.cli_text as cli_text
import sys
from glutil import Partitioner, GlutilError, DatabaseCleaner
from glutil.utils import print_batch_errors
from .serverless_function import create_found_partitions


def add_database_arg(parser):
    parser.add_argument(
        "database",
        type=str,
        default="",
        help="The Glue database containing the table you want to search for partitions")


def add_table_arg(parser):
    parser.add_argument(
        "table",
        type=str,
        help="The Glue table you want to search for partitions")


def add_flag_args(parser):
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform a \"dry-run\", show the actions to be taken without actually performing them.")
    parser.add_argument("--profile", "-p", type=str, help="AWS profile to use")


class Cli(object):
    def main(self, passed_args=None):
        parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description="Manage S3-backed Glue tables")

        subparsers = parser.add_subparsers(dest="cmd")

        create_partitions_parser = subparsers.add_parser(
            "create-partitions",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=cli_text.create_partitions_help,
            help="Create partitions in the glue catalog based on what's in S3")
        add_database_arg(create_partitions_parser)
        add_table_arg(create_partitions_parser)
        add_flag_args(create_partitions_parser)
        create_partitions_parser.add_argument(
            "--limit-days",
            "-l",
            type=int,
            default=0,
            help="Limit the number of days in the past to search for new partitions. (0 = no limit)")

        delete_all_partitions_parser = subparsers.add_parser(
            "delete-all-partitions",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=cli_text.delete_all_partitions_help,
            help="Remove all partitions from a specified Glue table.")
        add_database_arg(delete_all_partitions_parser)
        add_table_arg(delete_all_partitions_parser)
        add_flag_args(delete_all_partitions_parser)

        delete_bad_partitions_parser = subparsers.add_parser(
            "delete-bad-partitions",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            help="Remove partitions that don't match those found in S3",
            description=cli_text.delete_bad_partitions_help)
        add_database_arg(delete_bad_partitions_parser)
        add_table_arg(delete_bad_partitions_parser)
        add_flag_args(delete_bad_partitions_parser)

        delete_missing_partitions_parser = subparsers.add_parser(
            "delete-missing-partitions",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            help="Remove partitions from the catalog that don't exist in S3",
            description=cli_text.delete_missing_partitions_help)
        add_database_arg(delete_missing_partitions_parser)
        add_table_arg(delete_missing_partitions_parser)
        add_flag_args(delete_missing_partitions_parser)

        update_partitions_parser = subparsers.add_parser(
            "update-partitions",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            help="After moving the backing data in S3, update the catalog's partitions to match the new location",
            description=cli_text.update_partitions_help)
        add_database_arg(update_partitions_parser)
        add_table_arg(update_partitions_parser)
        add_flag_args(update_partitions_parser)

        delete_bad_tables_parser = subparsers.add_parser(
            "delete-bad-tables",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            help="Remove tables erroneously created by a Glue Crawler",
            description=cli_text.delete_bad_tables_help)
        add_database_arg(delete_bad_tables_parser)
        add_flag_args(delete_bad_tables_parser)

        if passed_args:
            args = parser.parse_args(passed_args)
        else:
            args = parser.parse_args()  # pragma: no cover

        func = args.cmd.replace("-", "_")
        getattr(self, func)(args)

    def create_partitions(self, args):
        partitioner = self.get_partitioner(args)
        create_found_partitions(partitioner, dry_run=args.dry_run, limit_days=args.limit_days)

    def delete_all_partitions(self, args):
        partitioner = self.get_partitioner(args)

        existing = partitioner.existing_partitions()

        if existing:
            print("Deleting the following partitions:")
            for partition in existing:
                print(f"\t{str(partition)}")
        else:
            print(f"No partitions found in table {args.table}")

        if not args.dry_run:
            errors = partitioner.delete_partitions(existing)
            if errors:
                print_batch_errors(errors)
                sys.exit(1)

    def delete_bad_partitions(self, args):
        partitioner = self.get_partitioner(args)

        to_delete = partitioner.bad_partitions()
        print("Found {} partitions to delete".format(len(to_delete)))

        if to_delete:
            print("Deleting the following partitions:")
            for partition in to_delete:
                print(f"\t{str(partition)}")

            if not args.dry_run:
                errors = partitioner.delete_partitions(to_delete)
                if errors:
                    print_batch_errors(errors)
                    sys.exit(1)

    def delete_bad_tables(self, args):
        cleaner = self.get_database_cleaner(args)
        to_delete = cleaner.child_tables()

        if not to_delete:
            print("Nothing to delete")
        else:
            print("Going to delete the following tables:")
            for table in to_delete:
                print(f"\t{table}")

            if not args.dry_run:
                errors = cleaner.delete_tables(to_delete)
                if errors:
                    print_batch_errors(errors, obj_type="tables", obj_key="TableName")
                    sys.exit(1)

    def delete_missing_partitions(self, args):
        partitioner = self.get_partitioner(args)

        to_delete = partitioner.missing_partitions()
        print("Found {} partitions to delete:".format(len(to_delete)))
        for partition in to_delete:
            print(f"\t{str(partition)}")

        if not args.dry_run:
            errors = partitioner.delete_partitions(to_delete)
            if errors:
                print_batch_errors(errors)
                sys.exit(1)

    def update_partitions(self, args):
        partitioner = self.get_partitioner(args)
        moved = partitioner.find_moved_partitions()

        if not moved:
            print("No partitions to update")
            return

        print(f"Found {len(moved)} moved partitions")
        for partition in moved:
            print(f"\t{partition}")

        if not args.dry_run:
            errors = partitioner.update_partition_locations(moved)
            if errors:
                print_batch_errors(errors, action="update")
                sys.exit(1)

    def get_partitioner(self, args):
        try:
            return Partitioner(args.database, args.table, aws_profile=args.profile)
        except GlutilError as e:
            message = e.message
            if e.error_type == "ProfileNotFound":
                message += f"\n\tConfirm that {args.profile} is a locally configured aws profile."
            if e.error_type == "AccessDenied":
                if args.profile:
                    message += f"\n\tConfirm that {args.profile} has the glue:GetTable permission."
                else:
                    message += "\n\tDid you mean to run this with a profile specified?"
            if e.error_type == "EntityNotFound":
                message += f"\n\tConfirm {args.table} exists, and you have the ability to access it."

            print(message)
            sys.exit(1)

    def get_database_cleaner(self, args):
        try:
            return DatabaseCleaner(args.database, aws_profile=args.profile)
        except GlutilError as e:  # pragma: no cover
            message = e.message
            if e.error_type == "AccessDenied":
                if args.profile:
                    message += f"\n\tConfirm that {args.profile} has the glue:GetTable permission."
                else:
                    message += "\n\tDid you mean to run this with a profile specified?"
            if e.error_type == "EntityNotFound":
                message += f"\n\tConfirm {args.table} exists, and you have the ability to access it."

            print(message)
            sys.exit(1)
