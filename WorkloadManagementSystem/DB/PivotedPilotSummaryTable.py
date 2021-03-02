from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
import DIRAC.Core.Utilities.Time as Time
from DIRAC.Core.Utilities.MySQL import _quotedList


class PivotedPilotSummaryTable:
  """
    SELECT Pivoted_eff.GridSite, Pivoted_eff.DestinationSite, Pivoted_eff.Done_Empties,
           Pivoted_eff.Submitted, Pivoted_eff.Done, Pivoted_eff.Failed, Pivoted_eff.Aborted,
           Pivoted_eff.Running, Pivoted_eff.Waiting, Pivoted_eff.Scheduled, Pivoted_eff.Total,
    (CASE
         WHEN Pivoted_eff.Done - Pivoted_eff.Done_Empties > 0
              THEN Pivoted_eff.Done/(Pivoted_eff.Done-Pivoted_eff.Done_Empties)
         WHEN Pivoted_eff.Done=0
              THEN 0
         WHEN Pivoted_eff.Done=Pivoted_eff.Done_Empties
              THEN 99.0 ELSE 0.0 END)
          AS PilotsPerJob,
          (Pivoted_eff.Total - Pivoted_eff.Aborted)/Pivoted_eff.Total*100.0 AS Eff
    FROM (SELECT Frank.GridSite, Frank.DestinationSite,
                 SUM(if (Frank.Status='Done', Frank.Empties,0)) AS Done_Empties,
                 SUM(if (Frank.Status='Submitted', Frank.qty, 0)) AS Submitted,
                 SUM(if (Frank.Status='Done', Frank.qty, 0)) AS Done,
                 SUM(if (Frank.Status='Failed', Frank.qty, 0)) AS Failed,
                 SUM(if (Frank.Status='Aborted', Frank.qty, 0)) AS Aborted,
                 SUM(if (Frank.Status='Running', Frank.qty, 0)) AS Running,
                 SUM(if (Frank.Status='Waiting', Frank.qty, 0)) AS Waiting,
                 SUM(if (Frank.Status='Scheduled', Frank.qty, 0)) AS Scheduled,
                 SUM(Frank.qty) AS Total
          FROM (SELECT GridSite, DestinationSite, Status, count(CASE WHEN CurrentJobID=0  THEN 1 END) AS Empties,
                count(*) AS qty
                from PilotAgents where
                     Status not in ('Done', 'Aborted') OR (Status in ('Done', 'Aborted')
                     AND LastUpdateTime > '2019-12-04 10:35:00')  group by GridSite, DestinationSite, Status)
          AS Frank
          group by GridSite, DestinationSite)
    AS Pivoted_eff;
  """

  def __init__(self, columnList):
    """
    Initialise a table with columns to be grouped by.

    :param columnList: i.e. ['GridSite', 'DestinationSite']
    :return:
    """

    self.columnList = columnList
    self.pstates = ['Submitted', 'Done', 'Failed', 'Aborted',
                    'Running', 'Waiting', 'Scheduled', 'Ready']

    # we want 'Site' and 'CE' in the final result
    colMap = {'GridSite': 'Site', 'DestinationSite': 'CE'}
    self.columns = [colMap.get(val, val) for val in columnList]

    self.columns += self.pstates  # MySQL._query() does not give us column names, sadly.

  def buildSQL(self, selectDict=None):
    """
    Build an SQL query to create a table with all status counts in one row, ("pivoted")
    grouped by columns in the column list.

    :param selectDict:
    :return: SQL query
    """

    last_update = Time.dateTime() - Time.day

    pvtable = 'pivoted'
    innerGroupBy = "(SELECT %s, Status,\n " \
                   "count(CASE WHEN CurrentJobID=0  THEN 1 END) AS Empties," \
                   " count(*) AS qty FROM PilotAgents\n " \
                   "WHERE Status NOT IN ('Done', 'Aborted') OR (Status in ('Done', 'Aborted') \n" \
                   " AND \n" \
                   " LastUpdateTime > '%s')" \
                   " GROUP by %s, Status)\n AS %s" % (
                       _quotedList(self.columnList), last_update,
                       _quotedList(self.columnList), pvtable)

    # pivoted table: combine records with the same group of self.columnList into a single row.

    # FIXME backqouting does not work with a prefix. Investigate.
    pivotedQuery = "SELECT %s,\n" % ', '.join([pvtable + '.' + item for item in self.columnList])
    line_template = " SUM(if (pivoted.Status={state!r}, pivoted.qty, 0)) AS {state}"
    pivotedQuery += ',\n'.join(line_template.format(state=state) for state in self.pstates)
    pivotedQuery += ",\n  SUM(if (%s.Status='Done', %s.Empties,0)) AS Done_Empty,\n" \
                    "  SUM(%s.qty) AS Total " \
                    "FROM\n" % (pvtable, pvtable, pvtable)

    outerGroupBy = " GROUP BY %s) \nAS pivoted_eff;" % _quotedList(self.columnList)

    # add efficiency columns using aliases defined in the pivoted table
    eff_case = "(CASE\n  WHEN pivoted_eff.Done - pivoted_eff.Done_Empty > 0 \n" \
               "  THEN pivoted_eff.Done/(pivoted_eff.Done-pivoted_eff.Done_Empty) \n" \
               "  WHEN pivoted_eff.Done=0 THEN 0 \n" \
               "  WHEN pivoted_eff.Done=pivoted_eff.Done_Empty \n" \
               "  THEN 99.0 ELSE 0.0 END) AS PilotsPerJob,\n" \
               " (pivoted_eff.Total - pivoted_eff.Aborted)/pivoted_eff.Total*100.0 AS PilotJobEff \nFROM \n("
    eff_select_template = " CAST(pivoted_eff.{state} AS UNSIGNED) AS {state} "
#    eff_select_template = " CAST(pivoted_eff.{state} AS UNSIGNED) AS {state} "
    # now select the columns + the states:
    pivoted_eff = "SELECT %s,\n" % ', '.join(['pivoted_eff' + '.' + item for item in self.columnList]) + \
                  ', '.join(eff_select_template.format(state=state) for state in self.pstates + ['Total']) + ", \n"

    finalQuery = pivoted_eff + eff_case + pivotedQuery + innerGroupBy + outerGroupBy
    self.columns += [' Total', 'PilotsPerJob', 'PilotJobEff']
    return finalQuery

  def getColumnList(self):

    return self.columns
