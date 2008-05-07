
from DIRAC.AccountingSystem.private.Plotters.DataOperationPlotter import DataOperationPlotter
from DIRAC.AccountingSystem.private.Plotters.JobPlotter import JobPlotter

gPlottersList = {
                 'DataOperation' : DataOperationPlotter,
                 'Job' : JobPlotter
                 }