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

    res = generateHistogram(filename, [2, 2, 3, 4, 5, 5], {})
    assert res["OK"] is True

    res = compareToReferences(filename, referenceImages(plots_directory, "histogram1"))
    assert res == 0.0

    res = generateHistogram(
        filename, [{"a": [1, 2, 3, 1, 2, 2, 4, 2]}, {"b": [2, 2, 2, 4, 4, 1, 1]}], {"plot_grid": "2:1"}
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
