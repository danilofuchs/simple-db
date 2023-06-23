from query import Where, parse_where


def test_parse_where():
    assert parse_where("id = 1") == Where(left_hand="id", right_hand="1", operator="=")


def test_parse_where_str():
    assert parse_where("name = 'Fuchs'") == Where(
        left_hand="name", right_hand="'Fuchs'", operator="="
    )


def test_parse_where_no_space():
    assert parse_where("name='Fuchs'") == Where(
        left_hand="name", right_hand="'Fuchs'", operator="="
    )
