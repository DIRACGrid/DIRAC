"""Tests for saferEval – uses pytest parametrize for conciseness."""

import time

import pytest

from DIRAC.Core.Utilities.SaferEval import saferEval


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        False,
        0,
        42,
        -17,
        0xFF,
        0o77,
        0b1010,
        3.14,
        1e10,
        1j,
        [],
        [1, "two", True, None],
        (),
        (1,),
        (1, 2, 3),
        {},
        {"a": 1, "b": 2},
        {1, 2, 3},
        [[1, 2], [3, 4]],
        {"a": {"b": {"c": [1, 2]}}},
    ],
)
def test_literal(value):
    assert saferEval(str(value)) == value


@pytest.mark.parametrize(
    "input_str,expected",
    [
        ('"hello"', "hello"),
        ("'hello'", "hello"),
        ("'a\\nb'", "a\nb"),
        (r"r'\n'", r"\n"),
        ('"hello 🌍"', "hello 🌍"),
        ("b'bytes'", b"bytes"),
        ("b'\\xff'", b"\xff"),
    ],
)
def test_string_literal(input_str, expected):
    assert saferEval(input_str) == expected


@pytest.mark.parametrize(
    "input_str",
    [
        "__import__('os').system('id')",
        "open('/etc/passwd').read()",
        "list()",
        "foo",
        "datetime.datetime.now()",
        "lambda x: x",
        "{k: v for k, v in []}",
        "(x for x in [])",
        "x == y",
        "1 + 2",
        "().__class__",
        "x[0]",
        "[1,2][1:]",
        "*1",
        "builtins.open",
        "object()",
        "MyList()",
        "f'{1+2}'",
        "@decorator",
        "assert True",
        "return 42",
        "x += 1",
        "with open('x') as f: pass",
        "for x in []: pass",
        "try: pass\nexcept: pass",
        "import os",
        "from os import path",
        "del x",
        "raise ValueError('x')",
        "yield 1",
        "await something",
        "(x := 1)",
        "(lambda x, /: x)(1)",
        "10**200",
    ],
)
def test_rejected_inputs(input_str):
    with pytest.raises(ValueError):
        saferEval(input_str)


def test_max_len_exceeded():
    with pytest.raises(ValueError):
        saferEval("1" * 2049, 2048)


def test_max_len_custom_exceeded():
    with pytest.raises(ValueError):
        saferEval("[1, 2, 3]", 5)


def test_max_len_custom_ok():
    assert saferEval("[1, 2, 3]", 10) == [1, 2, 3]


def test_max_len_boundary_default():
    assert saferEval("42") == 42


@pytest.mark.parametrize("depth", [2000, 500])
def test_deep_nesting(depth):
    with pytest.raises((ValueError, RecursionError)):
        saferEval("[" * depth + "1" + "]" * depth)


def test_large_string_literal():
    with pytest.raises(ValueError):
        saferEval("'" + "a" * 3000 + "'", 2048)


def test_large_list():
    with pytest.raises(ValueError):
        saferEval(str([1] * 3000), 2048)


@pytest.mark.parametrize(
    "s",
    [
        "{" + ", ".join(f'"k{i}": {i}' for i in range(50)) + "}",
        "[" + ",".join(str(i) for i in range(50)) + "]",
    ],
)
def test_performance(s):
    start = time.time()
    saferEval(s, 2048)
    assert time.time() - start < 0.1
