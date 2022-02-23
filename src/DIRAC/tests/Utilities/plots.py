# pylint: disable=protected-access
import math
import operator
from functools import reduce

from PIL import Image


def compare(file1Path, file2Path):
    """
    Function used to compare two plots
    returns 0.0 if both are identical

    :param str file1Path: Path to the file1.
    :param str file2Path: Path to the file2.

    :return: float value rms.
    """
    # Crops image to remove the "Generated on xxxx UTC" string
    image1 = Image.open(file1Path).crop((0, 0, 800, 570))
    image2 = Image.open(file2Path).crop((0, 0, 800, 570))

    h1 = image1.histogram()
    h2 = image2.histogram()
    rms = math.sqrt(reduce(operator.add, map(lambda a, b: (a - b) ** 2, h1, h2)) / len(h1))
    return rms
