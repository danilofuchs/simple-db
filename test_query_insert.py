from query_insert import Insert, parse_insert


def test_insert():
    assert parse_insert("INSERT INTO users (id, name) VALUES (1, 'John')") == Insert(
        table="users",
        fields=["id", "name"],
        values=[1, "John"],
    )


def test_lowercase():
    assert parse_insert("insert into users (id, name) values (1, 'John')") == Insert(
        table="users",
        fields=["id", "name"],
        values=[1, "John"],
    )


def test_anycase():
    assert parse_insert("Insert intO users (id, name) values (1, 'John')") == Insert(
        table="users",
        fields=["id", "name"],
        values=[1, "John"],
    )


def test_no_whitespace():
    assert parse_insert("INSERT INTO users(id,name) VALUES(1,'John')") == Insert(
        table="users",
        fields=["id", "name"],
        values=[1, "John"],
    )
