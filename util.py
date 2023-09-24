def with_default(value, default_value):
    return value if value else default_value


def if_then_else(condition, true_value, false_value):
    return true_value if condition else false_value


def clamp(value, min_value, max_value):
    return min(max(value, min_value), max_value)


class Logger(object):
    def __init__(self, verbosity: int):
        self.verbosity = verbosity

    def log(self, message, verbosity=1):
        if self.verbosity >= verbosity:
            print(message)
