from query_select import OrderBy, Select, Where, parse_select


def test_select_star():
    assert parse_select("SELECT * FROM users") == Select(
        fields=["*"],
        table="users",
        where=None,
        order_by=None,
    )


def test_lowercase():
    assert parse_select("select * from users") == Select(
        fields=["*"],
        table="users",
        where=None,
        order_by=None,
    )


def test_anycase():
    assert parse_select("Select * FroM users") == Select(
        fields=["*"],
        table="users",
        where=None,
        order_by=None,
    )


def test_select_fields():
    assert parse_select("SELECT id, name FROM users") == Select(
        fields=["id", "name"],
        table="users",
        where=None,
        order_by=None,
    )


def test_select_fields_no_spaces():
    assert parse_select("SELECT id,name FROM users") == Select(
        fields=["id", "name"],
        table="users",
        where=None,
        order_by=None,
    )


def test_select_where_id_equals():
    assert parse_select("SELECT * FROM users WHERE id = 1") == Select(
        fields=["*"],
        table="users",
        where=Where(left_hand="id", right_hand="1", operator="="),
        order_by=None,
    )


def test_select_order_by():
    assert parse_select("SELECT * FROM users ORDER BY age ASC") == Select(
        fields=["*"],
        table="users",
        where=None,
        order_by=OrderBy(field="age", direction="asc"),
    )
    assert parse_select("SELECT * FROM users ORDER BY age DESC") == Select(
        fields=["*"],
        table="users",
        where=None,
        order_by=OrderBy(field="age", direction="desc"),
    )
