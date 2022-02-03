"""
This is a very simple service performance test. It calls the service with a message. The service
return the same message.
"""
import time

from DIRAC.Core.Base.Client import Client
import DIRAC

DIRAC.initialize()  # Initialize configuration

cl = Client(url="Framework/SystemAdministrator")


class Transaction(object):
    def __init__(self):
        self.custom_timers = {}

    def run(self):
        start_time = time.time()
        retVal = cl.echo("simple test")
        if not retVal["OK"]:
            print("ERROR", retVal["Message"])
        end_time = time.time()
        self.custom_timers["Service_ResponseTime"] = end_time - start_time
        self.custom_timers["Service_Echo"] = end_time - start_time


if __name__ == "__main__":
    trans = Transaction()
    trans.run()
    print(trans.custom_timers)
