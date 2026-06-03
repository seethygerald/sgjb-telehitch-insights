import pytest

from telehitch_insights.telegram_to_databricks import parse_table_name


def test_parse_table_name_quotes_valid_three_part_name():
    assert parse_table_name("main.default.telegram_messages") == "`main`.`default`.`telegram_messages`"


def test_parse_table_name_rejects_unsafe_identifier():
    with pytest.raises(ValueError):
        parse_table_name("main.default.telegram_messages;DROP TABLE users")
