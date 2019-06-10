from unittest import TestCase
from unittest.mock import MagicMock
from io import StringIO
import sys
import sure  # noqa: F401

from glutil.utils import paginated_response, print_batch_errors, grouper


class UtilTest(TestCase):
    def test_paginated_response(self):
        responses = [
            {"Items": [{"key": "item one"}], "NextToken": "abc123"},
            {"Items": [{"key": "item two"}]},
        ]

        mock = MagicMock()
        mock.side_effect = responses

        values = paginated_response(mock, {}, "Items")

        values.should.equal([
            {"key": "item one"},
            {"key": "item two"},
        ])

    def test_print_batch_errors(self):
        new_out = StringIO()
        old_out = sys.stdout

        try:
            sys.stdout = new_out

            errors = [{"Object": "some-value",
                       "ErrorDetail": {"ErrorCode": "SomeError"}}]
            print_batch_errors(errors, "object", "Object")

            output = new_out.getvalue().strip()
            output.should.equal(
                "One or more errors occurred when attempting to delete object\nError deleting some-value: SomeError")
        finally:
            sys.stdout = old_out

    def test_grouper(self):
        groups = list(grouper([1, 2, 3, 4, 5], 2))

        groups.should.have.length_of(3)
        groups[0].should.equal([1, 2])
        groups[1].should.equal([3, 4])
        groups[2].should.equal([5])
