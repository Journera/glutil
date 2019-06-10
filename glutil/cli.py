import argparse
import text.cli_text

def add_database_arg(parser):
    parser.add_argument(
        "database",
        type=str,
        default="",
        help="The Athena/Glue database containing the table you want to search for partitions")

def add_table_arg(parser):
    parser.add_argument(
        "table",
        type=str,
        help="The Athena/Glue table you want to search for partitions")


class Cli(object):
    def main(self):
        parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description="Manage S3-backed Glue/Athena tables")

        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Perform a \"dry-run\", show the actions to be taken without actually performing them.")
        parser.add_argument("--profile", "-p", type=str, help="AWS profile to use")


        subparsers = parser.add_subparsers(dest="cmd")

        delete_all_partitions_parser = subparsers.add_parser(
            "delete-all-partitions",
            help=text.cli_text.delete_all_partitions_help)
        add_database_arg(delete_all_partitions_parser)
        add_table_arg(delete_all_partitions_parser)

        delete_bad_partitions_parser = subparsers.add_parser(
            "delete-bad-partitions"
            help=text.cli_text.delete_bad_partitions_help)
        add_database_arg(delete_bad_partitions_parser)
        add_table_arg(delete_bad_partitions_parser)

        delete_bad_tables_parser = subparsers.add_parser(
            "delete-bad-tables",
            help=text.cli_text.delete_bad_tables_help)
        add_database_arg(delete_bad_tables_parser)

        delete_missing_partitions_parser = subparses.add_parser(
            "delete-missing-tables",
            help=text.cli_text.delete_missing_partitions_help)
        add_database_arg(delete_missing_partitions_parser)
        add_table_arg(delete_missing_partitions_parser)

        update_partitions_parser = subparsers.add_parser(
            "update-partitions",
            help=text.cli_text.update_partitions_help)
