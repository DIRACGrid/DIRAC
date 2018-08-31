
import csv
import matplotlib.pyplot as plt
import sys


# == BEGIN OF CONFIGURATION ==

# FOR DISPLAY - Enter settings relative to test
system = 'Test ping - timeout=30 - Diset Server'
multimech_thread = 60
multimech_time = 300
multimech_rampup = 200
multimech_clients = 8
server_maxThreads = 20

# For this line, use vmstat and copy the free memory
memoryOffset = 0

plt.suptitle(
    '%s with %d threads\n %d threads/client - %d clients (total %d threads) \n \
     duration: %dsec - rampup %dsec \n latency between client starts: 2s' %
    (system,
     server_maxThreads,
     multimech_thread,
     multimech_clients,
     multimech_clients * multimech_thread,
     multimech_time,
     multimech_rampup))


# END OF CONFIG


def get_results():
  if len(sys.argv) < 3:
    print "Usage: python plot-distributedTest NAME NUMBEROFFILE"
    print "Example: python plot-distributedTest 1532506328.38 2"
    sys.exit(1)

  file = sys.argv[1]
  count = int(sys.argv[2])
  results = []
  for i in range(1, count + 1):
    fileName = "%s.%s.txt" % (file, i)
    with open(fileName, 'r') as content:
      print "reading %s" % fileName
      lines = content.read().split('\n')[1:-1]
      result = [line.split(',') for line in lines]
      results.append(result)
      content.close()
  return results


def get_server_stats():
  print "Please specify location to file with server stats:"
  serverStatFile = "/tmp/results.txt"  # raw_input()
  print "Loading %s" % serverStatFile

  serverStats = dict()
  with open(serverStatFile, 'r') as content_file:
    lines = content_file.read().split('\n')[1:]
    for line in lines:
      line = line.split(';')
      serverStats[line[0]] = line[1:]
  return serverStats


def get_test_begin_end(results):
  """
    First result file contain the test started in first
    Last result file contain the test started in last
    Every test have same duration
    So we read first and last line to have begin hour and end hour
    (the '2' is because time is registered in the third row)
  """
  return (int(results[0][0][2]), int(results[-1][-1][2]))


def process_data(results, serverStats):
  # Begin and end are timestamps
  (begin, end) = get_test_begin_end(results)

  # Initializing all data list
  (time, requestTime, CPU, RAM, reqPerSec, errorRate, loadAvg) = ([], [], [], [], [], [], [])
  global memoryOffset
  initialRAM = memoryOffset

  for t in range(begin, end):  # We determine datas time with timestamp
    # Offset to set starttime = 0
    time.append(t - begin)

    # Getting requesttime (mean), number of request/error at a givent time
    (reqTime, reqCount, errorCount) = getRequestTimeAndCount(results, t)
    requestTime.append(reqTime)
    reqPerSec.append(reqCount)
    errorRate.append(errorCount)

    # Getting infos from Server, sometimes no info are getted during more than one second
    try:
      # Get CPU usage
      CPU.append(100 - int(serverStats[str(t)][14]))

      # Get Memory used (delta with memory at the beginning)
      usedRam = int(serverStats[str(t)][5]) - initialRAM
      RAM.append(usedRam)

      # Get the load
      loadAvg.append(100 * float(serverStats[str(t)][17]))  # 18
    except KeyError:
      # If fail in getting value, take previous values
      CPU.append(CPU[-1])
      RAM.append(RAM[-1])
      loadAvg.append(loadAvg[-1])

      print "ERROR - Some values missing for CPU and Memory usage [try to load for time=%s]" % t

  return (time, requestTime, CPU, RAM, reqPerSec, errorRate, loadAvg)


def getRequestTimeAndCount(data, time):
  reqCount = 0
  errorCount = 0
  totalRequest = 0.0

  for result in results:
    i = 0
    try:

      # Ignore past
      while int(result[i][2]) < time:
        i += 1

      # Get infos for present
      while int(result[i][2]) == time:
        reqCount += 1
        totalRequest += float(result[i][4])
        if result[i][5] != '':
          errorCount += 1
        i += 1
    except IndexError:
      pass
  return (totalRequest / reqCount if reqCount > 0 else 0, reqCount, errorCount)


def displayGraph(results, serverStats):
  """
    Display all the graph on the same figure
  """
  print "Processing data and plot, it may take some time for huge tests"
  (time, requestTime, CPU, RAM, reqPerSec, errorCount, loadAvg) = process_data(results, serverStats)

  plt.subplot(221)
  plt.plot(time, requestTime, '-', label="Request time (s)")
  plt.legend()
  plt.subplot(222)
  plt.plot(time, CPU, '*', label="CPU usage (%)")
  plt.plot(time, loadAvg, '*', label="Load average * 100")
  plt.legend()
  plt.subplot(223)
  plt.plot(time, RAM, '*', label="Used Memory (bytes)")
  plt.legend()
  plt.subplot(224)
  plt.plot(time, reqPerSec, '*', label="Requests/sec")
  plt.plot(time, errorCount, '*', label="Errors/sec")
  plt.legend()


results = get_results()
# process_data(results)
serverStats = get_server_stats()
displayGraph(results, serverStats)
plt.show()
