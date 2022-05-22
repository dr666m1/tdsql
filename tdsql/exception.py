class InvalidSql(Exception):
    """Raised when sql file has a problem"""


class InvalidYaml(Exception):
    """Raised when yaml file has a problem"""


class TdsqlAssertionError(Exception):
    """Raised when actual result and expected result do not match"""
