from itertools import zip_longest


class GlutilError(Exception):  # pragma: no cover
    def __init__(self, message=None, source=None):
        self.source = source
        self.message = message
        super().__init__(self, message)


def grouper(iterable, n):
    args = [iter(iterable)] * n
    return map(lambda x: list(filter(None, x)), zip_longest(*args))


def paginated_response(function, args, container_name):
    items = []
    next_token = None

    while True:
        if next_token:
            args["NextToken"] = next_token
        resp = function(**args)
        next_token = resp.get("NextToken", None)

        items.extend(resp[container_name])

        if not next_token:
            break

    return items


def print_batch_errors(errors, obj_type, obj_key):
    print("One or more errors occurred when attempting to delete", obj_type)

    for error in errors:
        print("Error deleting {}: {}".format(
            error[obj_key], error["ErrorDetail"]["ErrorCode"]))
