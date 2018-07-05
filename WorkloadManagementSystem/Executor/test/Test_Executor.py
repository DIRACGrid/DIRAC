""" pytest(s) for Executors
"""

# pylint: disable=protected-access, missing-docstring, invalid-name, line-too-long

import pytest

from DIRAC.WorkloadManagementSystem.Executor.JobScheduling import JobScheduling


@pytest.mark.parametrize("sites, banned, expected", [
    (['MY.Site1.org', 'MY.Site2.org'], None, ['MY.Site1.org', 'MY.Site2.org']),
    (['MY.Site1.org', 'MY.Site2.org'], ['MY.Site1.org', 'MY.Site2.org'], []),
    (['MY.Site1.org', 'MY.Site2.org'], ['MY.Site2.org'], ['MY.Site1.org']),
    (['MY.Site1.org', 'MY.Site2.org'], [], ['MY.Site1.org', 'MY.Site2.org']),
    ([], ['MY.Site1.org'], []),
    ([], [], []),
    (['MY.Site1.org', 'MY.Site2.org'], ['MY.Site1.org'], ['MY.Site2.org']),
    (['MY.Site1.org', 'MY.Site2.org'], ['MY.Site1.org', 'MY.Site3.org'], ['MY.Site2.org']),
    ([], ['MY.Site1.org', 'MY.Site3.org'], []),
    (['MY.Site1.org', 'MY.Site2.org'], ['MY.Site4.org'], ['MY.Site1.org', 'MY.Site2.org']),
    (['MY.Site1.org', 'MY.Site2.org', 'MY.Site3.org'], ['MY.Site4.org'],
     ['MY.Site1.org', 'MY.Site2.org', 'MY.Site3.org']),
    (['MY.Site1.org', 'MY.Site2.org'], ['MY.Site4.org'], ['MY.Site1.org', 'MY.Site2.org'])])
def test__applySiteFilter(sites, banned, expected):
  js = JobScheduling()
  filtered = js._applySiteFilter(sites, banned)
  assert set(filtered) == set(expected)
