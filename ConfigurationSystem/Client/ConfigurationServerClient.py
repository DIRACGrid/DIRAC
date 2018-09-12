"""
  Custom Client for Configuration System
"""

from DIRAC.Core.Base.Client import Client


class ConfigurationServerClient(Client):
  """
    Placeholder client to speak with ConfigurationServer.
  """

  def __init__(self, **kwargs):
    if 'url' not in kwargs:
      kwargs['url'] = 'Configuration/Server'
    super(ConfigurationServerClient, self).__init__(**kwargs)
