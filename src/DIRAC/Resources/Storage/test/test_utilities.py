from pytest import fixture, raises
import gfal2  # pylint: disable=import-error
from DIRAC.Resources.Storage.GFAL2_StorageBase import setGfalSetting


#########################################################################

# The following tests make sure that the setGfalSetting contextmanager
# works as expected and does not leak


@fixture
def ctx():
    """Generates a gfal2 context"""
    yield gfal2.creat_context()


def test_setGfalSetting_integer(ctx):
    """Test setting an integer"""

    # First, the option should not be defined
    with raises(gfal2.GError):
        ctx.get_opt_integer("PLUGIN", "OPTION")

    # Then we define it in the context manager
    with setGfalSetting(ctx, "PLUGIN", "OPTION", 1):
        assert ctx.get_opt_integer("PLUGIN", "OPTION") == 1

    # Once out of it, it should not be defined anymore
    with raises(gfal2.GError):
        ctx.get_opt_integer("PLUGIN", "OPTION")

    # Set a value to start with
    ctx.set_opt_integer("PLUGIN", "OPTION", 0)
    assert ctx.get_opt_integer("PLUGIN", "OPTION") == 0

    # Change the value in the context
    with setGfalSetting(ctx, "PLUGIN", "OPTION", 3):
        assert ctx.get_opt_integer("PLUGIN", "OPTION") == 3

    # Make sure we are back to the original value
    assert ctx.get_opt_integer("PLUGIN", "OPTION") == 0


def test_setGfalSetting_bool(ctx):
    """Test setting an boolean"""
    with raises(gfal2.GError):
        ctx.get_opt_boolean("PLUGIN", "OPTION")

    with setGfalSetting(ctx, "PLUGIN", "OPTION", True):
        assert ctx.get_opt_boolean("PLUGIN", "OPTION") is True

    with raises(gfal2.GError):
        ctx.get_opt_boolean("PLUGIN", "OPTION")

    ctx.set_opt_boolean("PLUGIN", "OPTION", False)
    assert ctx.get_opt_boolean("PLUGIN", "OPTION") is False

    with setGfalSetting(ctx, "PLUGIN", "OPTION", True):
        assert ctx.get_opt_boolean("PLUGIN", "OPTION") is True

    assert ctx.get_opt_boolean("PLUGIN", "OPTION") is False


def test_setGfalSetting_string(ctx):
    """Test setting an string"""
    with raises(gfal2.GError):
        ctx.get_opt_string("PLUGIN", "OPTION")

    with setGfalSetting(ctx, "PLUGIN", "OPTION", "toto"):
        assert ctx.get_opt_string("PLUGIN", "OPTION") == "toto"

    with raises(gfal2.GError):
        ctx.get_opt_string("PLUGIN", "OPTION")

    ctx.set_opt_string("PLUGIN", "OPTION", "")
    assert ctx.get_opt_string("PLUGIN", "OPTION") == ""

    with setGfalSetting(ctx, "PLUGIN", "OPTION", "tata"):
        assert ctx.get_opt_string("PLUGIN", "OPTION") == "tata"

    assert ctx.get_opt_string("PLUGIN", "OPTION") == ""


def test_setGfalSetting_string_list(ctx):
    """Test setting a string list"""
    with raises(gfal2.GError):
        ctx.get_opt_string_list("PLUGIN", "OPTION")

    with setGfalSetting(ctx, "PLUGIN", "OPTION", ["toto"]):
        assert ctx.get_opt_string_list("PLUGIN", "OPTION") == ["toto"]

    with raises(gfal2.GError):
        ctx.get_opt_string_list("PLUGIN", "OPTION")

    ctx.set_opt_string_list("PLUGIN", "OPTION", [])
    assert ctx.get_opt_string_list("PLUGIN", "OPTION") == []

    with setGfalSetting(ctx, "PLUGIN", "OPTION", ["tata"]):
        assert ctx.get_opt_string_list("PLUGIN", "OPTION") == ["tata"]

    assert ctx.get_opt_string_list("PLUGIN", "OPTION") == []


def test_setGfalSetting_nested(ctx):
    """Test setting an integer and a boolean"""

    # First, the option should not be defined
    with raises(gfal2.GError):
        ctx.get_opt_integer("PLUGIN", "INT")

    with raises(gfal2.GError):
        ctx.get_opt_boolean("PLUGIN", "BOOL")

    # Then we define it in the context manager
    with setGfalSetting(ctx, "PLUGIN", "INT", 1):
        with setGfalSetting(ctx, "PLUGIN", "BOOL", True):
            assert ctx.get_opt_boolean("PLUGIN", "BOOL") is True
            assert ctx.get_opt_integer("PLUGIN", "INT") == 1

    # Once out of it, it should not be defined anymore
    with raises(gfal2.GError):
        ctx.get_opt_integer("PLUGIN", "INT")

    with raises(gfal2.GError):
        ctx.get_opt_boolean("PLUGIN", "BOOL")

    # Set a value to start with
    ctx.set_opt_integer("PLUGIN", "INT", 0)
    assert ctx.get_opt_integer("PLUGIN", "INT") == 0

    ctx.set_opt_boolean("PLUGIN", "BOOL", False)
    assert ctx.get_opt_boolean("PLUGIN", "BOOL") is False

    # Change the value in the context
    with setGfalSetting(ctx, "PLUGIN", "INT", 3):
        with setGfalSetting(ctx, "PLUGIN", "BOOL", True):
            assert ctx.get_opt_integer("PLUGIN", "INT") == 3
            assert ctx.get_opt_boolean("PLUGIN", "BOOL") is True

    # Make sure we are back to the original value
    assert ctx.get_opt_integer("PLUGIN", "INT") == 0
    assert ctx.get_opt_boolean("PLUGIN", "BOOL") is False


def test_setGfalSetting_doubleWith(ctx):
    """Test setting an integer and a boolean"""

    # First, the option should not be defined
    with raises(gfal2.GError):
        ctx.get_opt_integer("PLUGIN", "INT")

    with raises(gfal2.GError):
        ctx.get_opt_boolean("PLUGIN", "BOOL")

    # Then we define it in the context manager
    with setGfalSetting(ctx, "PLUGIN", "INT", 1), setGfalSetting(ctx, "PLUGIN", "BOOL", True):
        assert ctx.get_opt_boolean("PLUGIN", "BOOL") is True
        assert ctx.get_opt_integer("PLUGIN", "INT") == 1

    # Once out of it, it should not be defined anymore
    with raises(gfal2.GError):
        ctx.get_opt_integer("PLUGIN", "INT")

    with raises(gfal2.GError):
        ctx.get_opt_boolean("PLUGIN", "BOOL")

    # Set a value to start with
    ctx.set_opt_integer("PLUGIN", "INT", 0)
    assert ctx.get_opt_integer("PLUGIN", "INT") == 0

    ctx.set_opt_boolean("PLUGIN", "BOOL", False)
    assert ctx.get_opt_boolean("PLUGIN", "BOOL") is False

    # Change the value in the context
    with setGfalSetting(ctx, "PLUGIN", "INT", 3), setGfalSetting(ctx, "PLUGIN", "BOOL", True):
        assert ctx.get_opt_integer("PLUGIN", "INT") == 3
        assert ctx.get_opt_boolean("PLUGIN", "BOOL") is True

    # Make sure we are back to the original value
    assert ctx.get_opt_integer("PLUGIN", "INT") == 0
    assert ctx.get_opt_boolean("PLUGIN", "BOOL") is False


def test_setGfalSetting_unknownType(ctx):
    """Test passing an unknown type"""
    with raises(NotImplementedError):
        with setGfalSetting(ctx, "PLUGIN", "OPT", dict()):
            pass


#########################################################################
