from query import Where
from query_update import Update, parse_update


def test_update():
    assert parse_update("UPDATE users SET name = 'John'") == Update(
        table="users",
        fields=["name"],
        values=["'John'"],
        where=None,
    )


def test_lowercase():
    assert parse_update("update users set name = 'John'") == Update(
        table="users",
        fields=["name"],
        values=["'John'"],
        where=None,
    )


def test_anycase():
    assert parse_update("Update users SeT name = 'John'") == Update(
        table="users",
        fields=["name"],
        values=["'John'"],
        where=None,
    )


def test_where():
    assert parse_update("UPDATE users SET name = 'John' WHERE id = 1") == Update(
        table="users",
        fields=["name"],
        values=["'John'"],
        where=Where(
            left_hand="id",
            right_hand="1",
            operator="=",
            or_where=None,
            and_where=None,
        ),
    )


def test_where_str():
    assert parse_update(
        "UPDATE users SET name = 'John' WHERE name = 'Johnson'"
    ) == Update(
        table="users",
        fields=["name"],
        values=["'John'"],
        where=Where(
            left_hand="name",
            right_hand="'Johnson'",
            operator="=",
            or_where=None,
            and_where=None,
        ),
    )


def test_two_fields():
    assert parse_update(
        "UPDATE users SET name = 'John', age = 18 WHERE id = 1"
    ) == Update(
        table="users",
        fields=["name", "age"],
        values=["'John'", "18"],
        where=Where(
            left_hand="id",
            right_hand="1",
            operator="=",
            or_where=None,
            and_where=None,
        ),
    )
