"""
  A little script to analyze multi-mechanize tests by ploting every test in a single figure

  Because it can't be automatized you have to define path of the folder who contains results

  testTornado and testDirac must have the same length
"""
import csv
import matplotlib.pyplot as plt

testTornado = ['ping/results/results_2018.06.22_14.50.53', 'service/results/results_2018.06.22_15.24.18']
testDirac = ['ping/results/results_2018.06.22_14.52.09', 'service/results/results_2018.06.22_14.55.03']


def read_data(test, groupSize):
  with open(test + '/results.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')

    sumgrouptime = 0
    sumgrouprequestTime = 0
    count = 0

    time = []
    requestTime = []

    for row in reader:
      count += 1
      sumgrouptime += float(row[1])
      sumgrouprequestTime += float(row[4])
      # We group some points to make graph readable
      if(count == groupSize):
        time.append(sumgrouptime / groupSize)
        requestTime.append(sumgrouprequestTime / groupSize)
        sumgrouptime = 0
        sumgrouprequestTime = 0
        count = 0
  return (time, requestTime)


def displayGraph(testTornado, testDirac, subplot, groupSize):
  plt.subplot(subplot)
  plt.ylabel('red = dirac')

  (timeTornado, requestTimeTornado) = read_data(testTornado, groupSize)
  (timeDirac, requestTimeDirac) = read_data(testDirac, groupSize)

  plt.plot(timeTornado, requestTimeTornado, 'b-', timeDirac, requestTimeDirac, 'r-')


# The "100*len(testTornado)+11+i" can look strange but it define a sublot dynamically
for i in range(len(testTornado)):
  displayGraph(testTornado[i], testDirac[i], 100 * len(testTornado) + 11 + i, 42)
plt.show()
