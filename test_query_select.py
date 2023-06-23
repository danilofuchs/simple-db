from query import Where
from query_select import OrderBy, Select, parse_select


def test_select_star():
    assert parse_select("SELECT * FROM users") == Select(
        fields=["*"],
        table="users",
        join_table=None,
        join_on=None,
        where=None,
        order_by=None,
        limit=None,
    )


def test_lowercase():
    assert parse_select("select * from users") == Select(
        fields=["*"],
        table="users",
        join_table=None,
        join_on=None,
        where=None,
        order_by=None,
        limit=None,
    )


def test_anycase():
    assert parse_select("Select * FroM users") == Select(
        fields=["*"],
        table="users",
        join_table=None,
        join_on=None,
        where=None,
        order_by=None,
        limit=None,
    )


def test_select_fields():
    assert parse_select("SELECT id, name FROM users") == Select(
        fields=["id", "name"],
        table="users",
        join_table=None,
        join_on=None,
        where=None,
        order_by=None,
        limit=None,
    )


def test_select_fields_no_spaces():
    assert parse_select("SELECT id,name FROM users") == Select(
        fields=["id", "name"],
        table="users",
        join_table=None,
        join_on=None,
        where=None,
        order_by=None,
        limit=None,
    )


def test_select_where_id_equals():
    assert parse_select("SELECT * FROM users WHERE id = 1") == Select(
        fields=["*"],
        table="users",
        join_table=None,
        join_on=None,
        where=Where(
            left_hand="id",
            right_hand="1",
            operator="=",
            or_where=None,
            and_where=None,
        ),
        order_by=None,
        limit=None,
    )


def test_select_str():
    assert parse_select("SELECT * FROM users WHERE name = 'Fuchs'") == Select(
        fields=["*"],
        table="users",
        join_table=None,
        join_on=None,
        where=Where(
            left_hand="name",
            right_hand="'Fuchs'",
            operator="=",
            or_where=None,
            and_where=None,
        ),
        order_by=None,
        limit=None,
    )


def test_select_str_doublequote():
    assert parse_select('SELECT * FROM users WHERE name = "Fuchs"') == Select(
        fields=["*"],
        table="users",
        join_table=None,
        join_on=None,
        where=Where(
            left_hand="name",
            right_hand='"Fuchs"',
            operator="=",
            or_where=None,
            and_where=None,
        ),
        order_by=None,
        limit=None,
    )


def test_select_order_by():
    assert parse_select("SELECT * FROM users ORDER BY age ASC") == Select(
        fields=["*"],
        table="users",
        join_table=None,
        join_on=None,
        where=None,
        order_by=OrderBy(field="age", direction="asc"),
        limit=None,
    )
    assert parse_select("SELECT * FROM users ORDER BY age DESC") == Select(
        fields=["*"],
        table="users",
        join_table=None,
        join_on=None,
        where=None,
        order_by=OrderBy(field="age", direction="desc"),
        limit=None,
    )


def test_select_limit():
    assert parse_select("SELECT * FROM users LIMIT 10") == Select(
        fields=["*"],
        table="users",
        join_table=None,
        join_on=None,
        where=None,
        order_by=None,
        limit=10,
    )


def test_select_join():
    assert parse_select(
        "SELECT * FROM users JOIN addresses ON users.id = addresses.user_id"
    ) == Select(
        fields=["*"],
        table="users",
        join_table="addresses",
        join_on=Where(
            left_hand="users.id",
            operator="=",
            right_hand="addresses.user_id",
            or_where=None,
            and_where=None,
        ),
        where=None,
        order_by=None,
        limit=None,
    )


def test_select_join_fields():
    assert parse_select(
        "SELECT users.name, addresses.city FROM users JOIN addresses ON users.id = addresses.user_id"
    ) == Select(
        fields=["users.name", "addresses.city"],
        table="users",
        join_table="addresses",
        join_on=Where(
            left_hand="users.id",
            operator="=",
            right_hand="addresses.user_id",
            or_where=None,
            and_where=None,
        ),
        where=None,
        order_by=None,
        limit=None,
    )


def test_select_where_or():
    assert parse_select("SELECT * FROM users WHERE id = 1 OR id = 2") == Select(
        fields=["*"],
        table="users",
        join_table=None,
        join_on=None,
        where=Where(
            left_hand="id",
            right_hand="1",
            operator="=",
            or_where=Where(
                left_hand="id",
                right_hand="2",
                operator="=",
                or_where=None,
                and_where=None,
            ),
            and_where=None,
        ),
        order_by=None,
        limit=None,
    )


def test_complex_select():
    assert (
        parse_select(
            """
            SELECT employees.last_name, salaries.salary FROM employees
            JOIN salaries ON employees.emp_no = salaries.emp_no
            WHERE employees.gender = 'M'
            AND employees.hire_date > '1989-01-01'
            ORDER BY employees.hire_date DESC
            LIMIT 10
        """
        )
        == Select(
            fields=["employees.last_name", "salaries.salary"],
            table="employees",
            join_table="salaries",
            join_on=Where(
                left_hand="employees.emp_no",
                right_hand="salaries.emp_no",
                operator="=",
                or_where=None,
                and_where=None,
            ),
            where=Where(
                left_hand="employees.gender",
                right_hand="'M'",
                operator="=",
                or_where=None,
                and_where=Where(
                    left_hand="employees.hire_date",
                    right_hand="'1989-01-01'",
                    operator=">",
                    or_where=None,
                    and_where=None,
                ),
            ),
            order_by=OrderBy(
                field="employees.hire_date",
                direction="desc",
            ),
            limit=10,
        )
    )
