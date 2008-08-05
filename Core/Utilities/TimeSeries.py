""" Class responsible for handling time-series data and calculating trends.

"""

import datetime

class TimeSeries:

  ###########################################################################
  def __init__(self, maxEntries=False, maxAge=False, minAge=False):
    self.data = []
    self.maxEntries = maxEntries
    self.maxAge = maxAge
    self.minAge = minAge

  ###########################################################################
  def __repr__(self):
    return str(self.convertToList())

  ###########################################################################
  def __str__(self):
    return str(self.convertToList())

  ###########################################################################
  def __len__(self):
    return len(self.data)

  ###########################################################################
  def add(self, value, timestamp=False):
    now = datetime.datetime.utcnow()
    if not timestamp:
      timestamp = now

    # Update the list (we will process this item ourselves)
    self.update()

    # Test to see if it is too new
    if self.minAge and len(self.data) > 0:
      if abs(self.data[0][1] - timestamp) < self.minAge:
        return

    # Is it too old?
    if self.maxAge:
      if abs(now - timestamp) > self.maxAge:
        return

    # Do we need more room?
    if self.maxEntries:
      if len(self.data) >= self.maxEntries:
        self.data.pop()

    # Alright, add the thing
    self.data.insert(0, [value, timestamp])
    self.data.sort(self.compare)

  ###########################################################################
  def update(self, maxAge=False):
    # Figure out what age we are using
    if not maxAge:
      maxAge = self.maxAge
      if not maxAge:
        return

    # Since the list is already sorted, we will find the cut-off point and slice off the old stuff
    now = datetime.datetime.utcnow()
    for i in range(len(self.data)):
      if abs(now - self.data[i][1]) > maxAge:
        self.data = self.data[0:i]
        return

  ###########################################################################
  def getItems(self, numItems=False, oldestFirst=False):
    """ Retrieve up to numItems of the most recent entries (or oldest).
        Use this call (with numItems=False) to retrieve a copy of the data.
    """
    temp = self.data[:]
    if oldestFirst:
      temp.reverse()
    if not numItems:
      numItems = len(temp)
    return temp[0:numItems]

  ###########################################################################
  def getTimes(self, maxAge, oldestFirst=False):
    """ Retrieve all entries newer than maxAge (or older)
    """
    now = datetime.datetime.utcnow()
    for i in range(len(self.data)):
      if abs(now - self.data[i][1]) > maxAge:
        # Slice off the proper half
        if oldestFirst:
          return self.data[i:len(self.data):-1]
        else:
          return self.data[0:i]

    return self.data[:]

  ###########################################################################
  def getRange(self):
    """ Returns a tuple containing the oldest and newest timestamps
    """
    return (self.data[-1][1], self.data[0][1])

  ###########################################################################
  def convertToList(self, data=False):
    """ Returns a list of data (stripping the timestamp, but preserving order)
    """
    if not data:
      data = self.data[:]
    if not data:
      return []
    if not len(data):
      return []
    out = []
    for i in data:
      # Don't consider badly formed data (after all, we aren't processing a straightforward array)
      if len(i) > 1:
        out.append(i[0])
    return out

  ###########################################################################
  def compare(self, item1, item2):
    """ Callback function for list.sort()
    """
    if item1[1] > item2[1]:
      return -1
    elif item1[1] < item2[1]:
      return 1
    else:
      return 0

  ###########################################################################
  # These are just wrappers for the timedelta class
  def seconds(self, x):
    return datetime.timedelta(seconds=x)
  def minutes(self, x):
    return datetime.timedelta(seconds=x*60)
  def hours(self, x):
    return datetime.timedelta(seconds=x*3600)
  def days(self, x):
    return datetime.timedelta(days=x)
  def weeks(self,x):
    return datetime.timedelta(days=x*7)

  ###########################################################################
  # These convert timedelta objects into integer values
  def deltaToMilliseconds(self, delta):
    return (((delta.days * 86400) + delta.seconds) * 1000) + (delta.microseconds // 1000)
  def deltaToSeconds(self, delta):
    return (delta.days * 86400) + delta.seconds
  def deltaToMinutes(self, delta):
    return (delta.days * 1440) + (delta.seconds // 60)
  def deltaToHours(self, delta):
    return (delta.days * 24) + (delta.seconds // 3600)

  ###########################################################################
  def trend(self, maxAge=False, resolution='Seconds'):
    """ Considers a range of time 'maxAge' and returns the current data trend (as a slope)
    """
    resolveDict = {'Milliseconds' : self.deltaToMilliseconds, 'Seconds' : self.deltaToSeconds, 'Minutes' : self.deltaToMinutes, 'Hours' : self.deltaToHours}
    if resolution not in resolveDict:
      return 0

    # Select a data source
    if not maxAge:
      data = self.data[:]
    else:
      data = self.getTimes(maxAge)

    # Now let's compute a linear least-squares fit. Why that regression? Because it's easier than everything else :)
    # X = time, Y = value

    # Compute various sums
    n = len(data)
    sum_x = sum_y = sum_xx = sum_xy = 0
    for i in data:
      x = resolveDict[resolution](abs(data[0][1] - i[1]))
      sum_x += x
      sum_y += i[0]
      sum_xx += x * x
      sum_xy += x * i[0]

    #a_top = (sum_y * sum_xx) - (sum_x * sum_xy)
    # We only need the b term because it is the slope in a least-squares linear regression
    b_top = (n * sum_xy) - (sum_x * sum_y)
    bottom = (n * sum_xx) - (sum_x * sum_x)

    if bottom == 0:
      return 0

    # Negative sign will reverse the fact that we actually calculated these backwards :)
    return float(-b_top) / bottom

  ###########################################################################
  def avg(self, maxAge=False, versusTime=None):
    """ Returns the average value of the data.
        First, all of the data is summed.
        If versusTime=None, then the total amount of data is divided by the number of entries,
          thus giving value-per-entry averages
        Otherwise, the total amount of data is divided by the time over which it spans (in the given units),
          thus yielding value-per-time averages
    """
    if versusTime:
      resolveDict = {'Milliseconds' : self.deltaToMilliseconds, 'Seconds' : self.deltaToSeconds, 'Minutes' : self.deltaToMinutes, 'Hours' : self.deltaToHours}
      if versusTime not in resolveDict:
        return 0

    # Select a data source
    if not maxAge:
      data = self.data[:]
    else:
      data = self.getTimes(maxAge)
      
    # If we have no data to handle, just leave.
    # This will also avoid zero-division later on.
    n = len(data)
    if not n:
      return 0

    # Sum the data
    total = 0
    for i in data:
      total += i[0]
      
    # Calculate the average
    if versusTime:
      span = self.getRange()
      span = abs(span[1] - span[0])
      span = resolveDict[versusTime](span)
      if not span:
        # Note that even if we have data, span might still be zero.
        # We should still return zero.
        # This is because if we don't have a time span (one or more instantaneous data points),
        #   we can't generate a time-based average.
        return 0
      result = float(total) / span
    else:
      result = float(total) / n
      
    return result

  ###########################################################################
  def clear(self):
    self.data = []

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
