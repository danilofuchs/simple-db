from query import Where
from query_delete import Delete, parse_delete


def test_delete():
    assert parse_delete("DELETE FROM users") == Delete(
        table="users",
        where=None,
    )


def test_where():
    assert parse_delete("DELETE FROM users WHERE id = 1") == Delete(
        table="users",
        where=Where(left_hand="id", right_hand="1", operator="="),
    )


def test_lowercase():
    assert parse_delete("delete from users") == Delete(
        table="users",
        where=None,
    )


def test_where_str():
    assert parse_delete("DELETE FROM users WHERE name = 'Fuchs'") == Delete(
        table="users",
        where=Where(left_hand="name", right_hand="'Fuchs'", operator="="),
    )
