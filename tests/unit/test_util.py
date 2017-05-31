import pytest
from doubles import allow, expect
from proxysql_tools.util.bug1258464 import bug1258464


def test__bug1258464killer_when_count_of_COMMIT_less_then_100(mocker):
    pass
