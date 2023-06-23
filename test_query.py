import pytest
from query import Where, parse_where


def test_parse_where():
    assert parse_where("id = 1") == Where(
        left_hand="id",
        right_hand="1",
        operator="=",
        or_where=None,
        and_where=None,
    )


def test_parse_where_str():
    assert parse_where("name = 'Fuchs'") == Where(
        left_hand="name",
        right_hand="'Fuchs'",
        operator="=",
        or_where=None,
        and_where=None,
    )


def test_parse_where_no_space():
    assert parse_where("name='Fuchs'") == Where(
        left_hand="name",
        right_hand="'Fuchs'",
        operator="=",
        or_where=None,
        and_where=None,
    )


def test_parse_or():
    assert parse_where("id = 1 OR name = 'Fuchs'") == Where(
        left_hand="id",
        right_hand="1",
        operator="=",
        or_where=Where(
            left_hand="name",
            right_hand="'Fuchs'",
            operator="=",
            or_where=None,
            and_where=None,
        ),
        and_where=None,
    )


def test_parse_and():
    assert parse_where("id = 1 AND name = 'Fuchs'") == Where(
        left_hand="id",
        right_hand="1",
        operator="=",
        or_where=None,
        and_where=Where(
            left_hand="name",
            right_hand="'Fuchs'",
            operator="=",
            or_where=None,
            and_where=None,
        ),
    )


def test_not_many_conditions():
    with pytest.raises(ValueError):
        parse_where("id = 1 AND name = 'Fuchs' OR age = 18")
