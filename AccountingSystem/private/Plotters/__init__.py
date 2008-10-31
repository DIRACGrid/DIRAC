
from DIRAC.AccountingSystem.private.Plotters.DataOperationPlotter import DataOperationPlotter
from DIRAC.AccountingSystem.private.Plotters.JobPlotter import JobPlotter
from DIRAC.AccountingSystem.private.Plotters.WMSHistoryPlotter import WMSHistoryPlotter
from DIRAC.AccountingSystem.private.Plotters.PilotPlotter import PilotPlotter
from DIRAC.AccountingSystem.private.Plotters.SRMSpaceTokenDeploymentPlotter import SRMSpaceTokenDeploymentPlotter

gPlottersList = {
                 'DataOperation' : DataOperationPlotter,
                 'Job' : JobPlotter,
                 'WMSHistory' : WMSHistoryPlotter,
                 'Pilot' : PilotPlotter,
                 'SRMSpaceTokenDeployment' : SRMSpaceTokenDeploymentPlotter
                 }