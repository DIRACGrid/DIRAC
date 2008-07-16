
from DIRAC.AccountingSystem.private.Plotters.DataOperationPlotter import DataOperationPlotter
from DIRAC.AccountingSystem.private.Plotters.JobPlotter import JobPlotter
from DIRAC.AccountingSystem.private.Plotters.WMSHistoryPlotter import WMSHistoryPlotter

gPlottersList = {
                 'DataOperation' : DataOperationPlotter,
                 'Job' : JobPlotter,
                 'WMSHistory' : WMSHistoryPlotter
                 }