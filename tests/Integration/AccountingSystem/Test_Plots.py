""" It is used to test Plotting utilities used to create different plots.
"""

# pylint: disable=invalid-name,wrong-import-position
import os

# sut
from DIRAC.Core.Utilities.Plotting.Plots import (
    generateErrorMessagePlot,
    generateHistogram,
    generateNoDataPlot,
    generatePiePlot,
)
from DIRAC.tests.Utilities.plots import compareToReferences, referenceImages

plots_directory = os.path.join(os.path.dirname(__file__), "plots")
filename = "plot.png"


def test_histogram():
    """
    test histogram
    """

    # The bin count picked by the default "auto" comes from numpy and changed in numpy 2.1, which
    # would make the reference images valid for one numpy version only. These two plots therefore
    # ask for an explicit number of bins; the values are the ones "auto" picks with numpy 1.x, so
    # the resulting plots are unchanged. "auto" is still exercised by histogram3 below, which is
    # also the only shape used in production (see JobPlotter._plotHistogramCPUUsed).
    res = generateHistogram(filename, [2, 2, 3, 4, 5, 5], {"bins": 4})
    assert res["OK"] is True

    res = compareToReferences(filename, referenceImages(plots_directory, "histogram1"))
    assert res == 0.0

    res = generateHistogram(
        filename,
        [{"a": [1, 2, 3, 1, 2, 2, 4, 2]}, {"b": [2, 2, 2, 4, 4, 1, 1]}],
        {"bins": 6, "plot_grid": "2:1"},
    )
    assert res["OK"] is True

    res = compareToReferences(filename, referenceImages(plots_directory, "histogram2"))
    assert res == 0.0

    res = generateHistogram(filename, [{"a": [1]}, {"b": [2, 3, 3, 5, 5]}], {})
    assert res["OK"] is True

    res = compareToReferences(filename, referenceImages(plots_directory, "histogram3"))
    assert res == 0.0


def test_piechartplot():
    """
    test pie chart plots
    """
    res = generatePiePlot(filename, {"a": 16.0, "b": 56.0, "c": 15, "d": 20}, {})
    assert res["OK"] is True

    res = compareToReferences(filename, referenceImages(plots_directory, "piechart"))
    assert res == 0.0


def test_nodataplot():
    """
    Test no data plot
    """

    res = generateNoDataPlot(filename, {}, {"title": "Test plot"})
    assert res["OK"] is True
    res = compareToReferences(filename, referenceImages(plots_directory, "nodata"))
    assert res == 0.0


def test_error():
    """
    Test error message plot
    """

    res = generateErrorMessagePlot("testing error message")
    with open(filename, "wb") as out:
        out.write(res)

    res = compareToReferences(filename, referenceImages(plots_directory, "error"))
    assert res == 0.0
