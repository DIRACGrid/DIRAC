from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


from DIRAC.FrameworkSystem.private.monitoring.ColorGenerator import ColorGenerator


def test_getHexColor():
  cg = ColorGenerator()
  for i in range(2):
    assert cg.getHexColor() == "0000FF"
    assert cg.getHexColor() == "00FF00"
    assert cg.getHexColor() == "FF0000"
    assert cg.getHexColor() == "00FFFF"
    assert cg.getHexColor() == "FF00FF"
    assert cg.getHexColor() == "FFFF00"
    assert cg.getHexColor() == "007F7F"
    assert cg.getHexColor() == "7F007F"
    assert cg.getHexColor() == "7F7F00"
    assert cg.getHexColor() == "007FFF"
    assert cg.getHexColor() == "00FF7F"
    assert cg.getHexColor() == "7F7F7F"
    assert cg.getHexColor() == "7F00FF"
    assert cg.getHexColor() == "FF007F"
    assert cg.getHexColor() == "7F7FFF"
    for j in range(1000):
      assert len(cg.getHexColor()) == 6
    cg.reset()


def test_getFloatColor():
  cg = ColorGenerator()
  cg.reset()
  for i in range(2):
    assert cg.getFloatColor() == (0.0, 0.0, 1.0)
    assert cg.getFloatColor() == (0.0, 1.0, 0.0)
    assert cg.getFloatColor() == (1.0, 0.0, 0.0)
    assert cg.getFloatColor() == (0.0, 1.0, 1.0)
    assert cg.getFloatColor() == (1.0, 0.0, 1.0)
    assert cg.getFloatColor() == (1.0, 1.0, 0.0)
    assert cg.getFloatColor() == (0.0, 0.5, 0.5)
    assert cg.getFloatColor() == (0.5, 0.0, 0.5)
    assert cg.getFloatColor() == (0.5, 0.5, 0.0)
    assert cg.getFloatColor() == (0.0, 0.5, 1.0)
    assert cg.getFloatColor() == (0.0, 1.0, 0.5)
    assert cg.getFloatColor() == (0.5, 0.5, 0.5)
    assert cg.getFloatColor() == (0.5, 0.0, 1.0)
    assert cg.getFloatColor() == (1.0, 0.0, 0.5)
    assert cg.getFloatColor() == (0.5, 0.5, 1.0)
    for j in range(1000):
      result = cg.getFloatColor()
      assert len(result) == 3
      assert all(isinstance(x, float) for x in result)
    cg.reset()
