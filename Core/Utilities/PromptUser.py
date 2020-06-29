""" Utility for prompting users
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import S_OK, S_ERROR
from six.moves import input


def promptUser(message, choices=[], default='n', logger=None):
  """ Prompting users with message, choices by default are 'y', 'n'
  """
  if logger is None:
    from DIRAC import gLogger
    logger = gLogger

  if not choices:
    choices = ['y', 'n']
  if (choices) and (default) and (default not in choices):
    return S_ERROR("The default value is not a valid choice")
  choiceString = ''
  if choices and default:
    choiceString = '/'.join(choices).replace(default, '[%s]' % default)
  elif choices and (not default):
    choiceString = '/'.join(choices)
  elif (not choices) and (default):
    choiceString = '[%s]' % default

  while True:
    if choiceString:
      logger.notice('%s %s :' % (message, choiceString))
    elif default:
      logger.notice('%s %s :' % (message, default))
    else:
      logger.notice('%s :' % message)
    response = input('')
    if (not response) and (default):
      return S_OK(default)
    elif (not response) and (not default):
      logger.error("Failed to determine user selection")
      return S_ERROR("Failed to determine user selection")
    elif (response) and (choices) and (response not in choices):
      logger.notice('your answer is not valid')
      continue
    else:
      return S_OK(response)
