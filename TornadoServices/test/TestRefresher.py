import time

from DIRAC.ConfigurationSystem.private.Refresher import gRefresher, gConfigurationData
from DIRAC import gConfig



def test_configAutoRefreshed():
    gRefresher.autoRefreshAndPublish("https://localhost/Configuration/Server")
    gRefresher.enable()
    FirstConfig = gConfig.getValue('/DIRAC/Configuration/Random')
    time.sleep(10)
    NewConfig = gConfig.getValue('/DIRAC/Configuration/Random')
    assert NewConfig != FirstConfig

def test_configNotRefreshedWhenDisabled():
    #gRefresher.daemonize()
    gRefresher.disable()
    FirstConfig = gConfig.getValue('/DIRAC/Configuration/Random')
    time.sleep(10)
    NewConfig = gConfig.getValue('/DIRAC/Configuration/Random')
    NewConfig == FirstConfig
