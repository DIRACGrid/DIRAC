"""Tests for MySQL
"""

import pytest

from DIRAC.Core.Utilities.MySQL import MySQL


class TestCheckIdentifierValid:
    """Valid SQL identifiers: letters, digits, underscores; must start
    with a letter or underscore."""

    @pytest.mark.parametrize(
        "identifier",
        [
            "a",
            "z",
            "A",
            "Z",
            "_",
            "__",
            "_a",
            "_1",
            "abc",
            "ABC",
            "a1",
            "a_b",
            "a_1_b",
            "table_name",
            "myTable",
            "t1",
        ],
    )
    def test_valid(self, identifier):
        result = MySQL._checkIdentifier(identifier)
        assert result["OK"]
        assert result["Value"] == identifier


class TestCheckIdentifierInvalid:
    """Must reject identifiers that start with digits or contain forbidden
    characters. Covers SQL injection vectors."""

    @pytest.mark.parametrize(
        "identifier",
        [
            # basic rejection
            "1abc",
            "0abc",
            "-abc",
            "abc-def",
            "abc.def",
            "abc/def",
            "abc\\def",
            "abc\ndef",
            "abc\tdef",
            "abc`def",
            'abc"def',
            "abc'def",
            "abc def",
            "a,b",
            "a;b",
            "a(b)",
            "a[]",
            "",
            " ",
            "a b",
            "\t",
            "\n",
            # SQL injection: comment-based
            "abc--",
            "abc--drop_table",
            "abc/*comment*/def",
            "abc/*drop\ntable*/",
            "--comment",
            "/*comment*/abc",
            # SQL injection: semicolon-based
            "abc;DROP TABLE",
            "abc;--",
            "abc;SELECT",
            "abc;DELETE",
            "abc;INSERT",
            "abc;UPDATE",
            "abc;TRUNCATE",
            "abc;ALTER",
            "abc;CREATE",
            "abc;EXEC",
            "abc;EXECUTE",
            "abc;xp_cmdshell",
            "abc;shutdown",
            # SQL injection: null bytes
            "abc\x00def",
            "abc\x00",
            # URL-encoded injection
            "abc%27def",
            "abc%20def",
            "abc%2527def",
            "abc%2720def",
            # Backtick / double-quote tricks (MySQL identifier quoting)
            "`abc`",
            '`abc`,"def"',
            '`abc"`',
            '"abc"',
            # SQL injection: UNION / SELECT tricks
            "abc UNION SELECT",
            "abc OR 1=1",
            "abc AND 1=1",
            "abc XOR",
            "abc IN (1,2,3)",
            "abc LIKE '%'",
            "abc = 'x'",
            "abc != x",
            "abc IS NULL",
            "abc IS NOT NULL",
            "abc BETWEEN",
            "abc NOT IN",
            "abc IN (SELECT id FROM users)",
            # Function calls in identifier positions
            "abc(DROP TABLE)",
            "abc(CONCAT(1,2,3))",
            "abc(CHAR(0))",
            # String concatenation tricks
            "abc||def",
            "abc||",
            # Unicode tricks
            "abc\u200bdef",
            "abc\u200cdef",
            "abc\u00addef",
            # Whitespace obfuscation
            "\x0babc",
            "\x0cabc",
            "\rabc",
        ],
    )
    def test_invalid(self, identifier):
        result = MySQL._checkIdentifier(identifier)
        assert not result["OK"]


class TestCheckTypeValid:
    """SQL type names: must start with a letter; may optionally include a
    parenthesised length / precision list with only digits and (one) comma."""

    @pytest.mark.parametrize(
        "value_type",
        [
            "VARCHAR",
            "INT",
            "DECIMAL",
            "TIMESTAMP",
            "CHAR",
            "TEXT",
            "BIGINT",
            "SMALLINT",
            "FLOAT",
            "DOUBLE",
            "BOOLEAN",
            "DATE",
            "TIME",
            "YEAR",
            "TINYINT",
            "VARCHAR(64)",
            "INT(11)",
            "CHAR(1)",
            "DECIMAL(10)",
            "DECIMAL(10, 2)",
            "DECIMAL(10,2)",
            "INT(11,2)",
            "VARCHAR( 64)",
            "VARCHAR(64 )",
            "VARCHAR( 64 )",
            "DECIMAL( 10 , 2 )",
            "INT( 11 , 2 )",
            "varchar",
            "Varchar(64)",
            "VarChar( 64 , 2 )",
            "VARCHAR (64)",
            "VARCHAR ( 64 )",
            "DECIMAL (10, 2)",
            "INT ( 11 , 2 )",
        ],
    )
    def test_valid(self, value_type):
        result = MySQL._checkType(value_type)
        assert result["OK"]
        assert result["Value"] == value_type


class TestCheckTypeInvalid:
    """Must reject types that start with digits, contain letters inside
    parentheses, have too many arguments, or are otherwise malformed.
    Also covers SQL injection patterns."""

    @pytest.mark.parametrize(
        "value_type",
        [
            # basic rejection
            "123INT",
            "1INT",
            "VARCHAR(",
            "VARCHAR64)",
            "VARCHAR(64,)",
            "VARCHAR(64, 2, 3)",
            "VARCHAR(abc)",
            "INT()",
            "VARCHAR( )",
            "VARCHAR(,)",
            "VARCHAR(, 2)",
            "VARCHAR(64, )",
            "",
            " ",
            "VAR CHAR(64)",
            "VARCHAR(,2)",
            # SQL injection: comments
            "VARCHAR(64)--",
            "VARCHAR(64)/*comment*/",
            "VARCHAR(64)/*drop\ntable*/",
            "--VARCHAR",
            "/*evil*/VARCHAR",
            # SQL injection: semicolons
            "VARCHAR(64);DROP TABLE",
            "VARCHAR(64);--",
            "VARCHAR(64);SELECT 1",
            # URL-encoded injection
            "VARCHAR(64)%27",
            "VARCHAR(64)%20",
            "VARCHAR(64)%27DROP",
            # Null bytes
            "VARCHAR(64)\x00DROP",
            "VARCHAR(64)\x00",
            # Unicode tricks
            "VARCHAR(64)\u200bDROP",
            "VARCHAR(64)\xadDROP",
            # UNION / SELECT tricks
            "VARCHAR(64) UNION SELECT",
            "VARCHAR(64) OR 1=1",
            "VARCHAR(64) AND 1=1",
            # Backtick tricks
            "VARCHAR(64)`DROP`",
            "`VARCHAR(64)`",
            # Function calls inside parens
            "VARCHAR(CHAR(64))",
            "VARCHAR(CONCAT(64))",
            "VARCHAR(0x56415243484152)",
            # Whitespace obfuscation
            "VARCHAR(64)\x0bDROP",
            "VARCHAR(64)\rDROP",
            "VARCHAR(64)\nDROP",
            # String concatenation
            "VARCHAR(64)||DROP",
            "VARCHAR(64)||",
        ],
    )
    def test_invalid(self, value_type):
        result = MySQL._checkType(value_type)
        assert not result["OK"]
