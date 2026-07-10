# pylint: disable=protected-access
import glob
import math
import operator
import os
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


def referenceImages(directory, stem):
    """Return every reference image available for a given plot.

    The primary reference is ``<stem>.png``. Additional references may be added
    as ``<stem>.<tag>.png`` (for example ``histogram1.mpl-3.10.png``) when a new
    matplotlib version renders a plot slightly differently. Keeping several
    references side by side means a plot is accepted against any supported
    plotting-stack version, so an upgrade only needs an extra reference image
    rather than replacing the existing one (which would break older versions).

    :param str directory: Directory holding the reference images.
    :param str stem: Base name of the plot, without extension.
    :return: Sorted list of reference image paths.
    """
    paths = glob.glob(os.path.join(directory, f"{stem}.png"))
    paths += glob.glob(os.path.join(directory, f"{stem}.*.png"))
    return sorted(paths)


def compareToReferences(generatedPath, referencePaths):
    """Compare a generated plot against several candidate reference images.

    :param str generatedPath: Path to the freshly generated plot.
    :param referencePaths: Iterable of reference image paths to compare against.

    :return: The smallest RMS obtained against any of the references, so a plot
        is accepted as soon as it is identical (rms == 0.0) to one of them.
    """
    referencePaths = list(referencePaths)
    if not referencePaths:
        raise ValueError(f"No reference images provided for {generatedPath}")
    return min(compare(generatedPath, reference) for reference in referencePaths)
