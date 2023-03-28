from diraccfg import CFG

from DIRAC import gConfig
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData


def setupConfig(config):
    """Set up the configuration file

    :param str config: configuration content to load
    """
    gConfigurationData.localCFG = CFG()
    cfg = CFG()
    cfg.loadFromBuffer(config)
    gConfig.loadCFG(cfg)
