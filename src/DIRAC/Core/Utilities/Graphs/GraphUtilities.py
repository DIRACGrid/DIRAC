""" GraphUtilities is a a collection of utility functions and classes used
    in the DIRAC Graphs package.

    The DIRAC Graphs package is derived from the GraphTool plotting package of the
    CMS/Phedex Project by ... <to be added>
"""
import os
import time
import datetime
import calendar
import math
import pytz
import numpy

from matplotlib.pyplot import rcParams
from matplotlib.ticker import ScalarFormatter
from matplotlib.dates import (
    AutoDateLocator,
    AutoDateFormatter,
    DateFormatter,
    RRuleLocator,
    rrulewrapper,
    HOURLY,
    MINUTELY,
    SECONDLY,
    YEARLY,
    MONTHLY,
    DAILY,
)
from dateutil.relativedelta import relativedelta

# This is a hack to workaround the use of float(ScalarFormatter.__call__(...))
rcParams["axes.unicode_minus"] = False


def evalPrefs(*args, **kw):
    """Interpret arguments as preferencies dictionaries or key-value pairs. The overriding order
    is right most - most important one. Returns a single dictionary of preferencies
    """

    prefs = {}
    for pDict in list(args) + [kw]:
        if isinstance(pDict, dict):
            for key in pDict:
                if key == "metadata":
                    for mkey in pDict[key]:
                        prefs[mkey] = pDict[key][mkey]
                else:
                    prefs[key] = pDict[key]

    return prefs


def pixelToPoint(size, dpi):
    """Convert size expressed in pixels into points for a given dpi resolution"""

    return float(size) * 100.0 / float(dpi)


datestrings = ["%x %X", "%x", "%Y-%m-%d %H:%M:%S"]


def convert_to_datetime(dstring):
    orig_string = str(dstring)
    if isinstance(dstring, int) and dstring > 10**11:  # Check if timestamp is in milliseconds
        dstring /= 1000
    try:
        if isinstance(dstring, datetime.datetime):
            results = dstring
        else:
            results = eval(str(dstring), {"__builtins__": None, "time": time, "math": math}, {})
        if isinstance(results, (int, float)):
            # Use utcfromtimestamp for UTC time
            results = datetime.datetime.utcfromtimestamp(int(results))
        elif isinstance(results, datetime.datetime):
            if results.tzinfo is not None:
                # non-naive datetime: convert to UTC
                results = results.astimezone(datetime.timezone.utc)
            else:
                # DIRAC naive datetimes are UTC everywhere: add tzinfo
                results = results.replace(tzinfo=datetime.timezone.utc)
        else:
            raise ValueError("Unknown datetime type!")
    except Exception:
        t = None
        for dateformat in datestrings:
            try:
                t = time.strptime(dstring, dateformat)
                timestamp = calendar.timegm(t)  # Convert to UTC timestamp
                results = datetime.datetime.utcfromtimestamp(timestamp)
                break
            except Exception:
                pass
        if t is None:
            try:
                dstring = dstring.split(".", 1)[0]
                t = time.strptime(dstring, dateformat)
                timestamp = calendar.timegm(t)  # Convert to UTC timestamp
                results = datetime.datetime.utcfromtimestamp(timestamp)
            except Exception:
                raise ValueError(
                    "Unable to create time from string!\nExpecting "
                    "format of: '12/06/06 12:54:67'\nReceived:%s" % orig_string
                )
    return results


def to_timestamp(val):
    try:
        v = float(val)
        if v > 1000000000 and v < 1900000000:
            return v
    except Exception:
        pass

    val = convert_to_datetime(val)
    return calendar.timegm(val.timetuple())
    # return time.mktime(val.timetuple())


# If the graph has more than `hour_switch` minutes, we print
# out hours in the subtitle.
hour_switch = 7

# If the graph has more than `day_switch` hours, we print
# out days in the subtitle.
day_switch = 7

# If the graph has more than `week_switch` days, we print
# out the weeks in the subtitle.
week_switch = 7


def add_time_to_title(begin, end, metadata={}):
    """Given a title and two times, adds the time info to the title.
    Example results::

       "Number of Attempted Transfers
       (24 Hours from 4:45 12-14-2006 to 5:56 12-15-2006)"

    There are two important pieces to the subtitle we add - the duration
    (i.e., '48 Hours') and the time interval (i.e., 11:00 07-02-2007 to
    11:00 07-04-2007).

    We attempt to make the duration match the size of the span (for a bar
    graph, this would be the width of the individual bar) in order for it
    to make the most sense.  The formatting of the time interval is based
    upon how much real time there is from the beginning to the end.

    We made the distinction because some would want to show graphs
    representing 168 Hours, but needed the format to show the date as
    well as the time.
    """
    format_str = None
    if "span" in metadata:
        interval = metadata["span"]
    else:
        interval = time_interval(begin, end)
    formatting_interval = time_interval(begin, end)
    if formatting_interval == 600:
        format_str = "%H:%M:%S"
    elif formatting_interval == 3600:
        format_str = "%Y-%m-%d %H:%M"
    elif formatting_interval == 86400:
        format_str = "%Y-%m-%d"
    elif formatting_interval == 86400 * 7:
        format_str = "Week %U of %Y"

    if interval < 600:
        format_name = "Seconds"
        time_slice = 1
    elif interval < 3600 and interval >= 600:
        format_name = "Minutes"
        time_slice = 60
    elif interval >= 3600 and interval < 86400:
        format_name = "Hours"
        time_slice = 3600
    elif interval >= 86400 and interval < 86400 * 7:
        format_name = "Days"
        time_slice = 86400
    elif interval >= 86400 * 7:
        format_name = "Weeks"
        time_slice = 86400 * 7
    else:
        format_str = "%x %X"
        format_name = "Seconds"
        time_slice = 1

    begin_tuple = time.gmtime(begin)
    end_tuple = time.gmtime(end)
    added_title = "%i %s from " % (int((end - begin) / time_slice), format_name)
    added_title += time.strftime(f"{format_str} to", begin_tuple)
    if time_slice < 86400:
        add_utc = " UTC"
    else:
        add_utc = ""
    added_title += time.strftime(f" {format_str}{add_utc}", end_tuple)
    return added_title


def time_interval(begin, end):
    """
    Determine the appropriate time interval based upon the length of
    time as indicated by the `starttime` and `endtime` keywords.
    """

    if end - begin < 600 * hour_switch:
        return 600
    if end - begin < 86400 * day_switch:
        return 3600
    elif end - begin < 86400 * 7 * week_switch:
        return 86400
    else:
        return 86400 * 7


def comma_format(x_orig):
    x = float(x_orig)
    if x >= 1000:
        after_comma = x % 1000
        before_comma = int(int(x) / 1000)
        return f"{comma_format(before_comma)},{after_comma:03g}"
    else:
        return str(x_orig)


class PrettyScalarFormatter(ScalarFormatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_powerlimits([-7, 9])
        self._useLocale = True

    def __call__(self, x, pos=None):
        val = super().__call__(x, pos=pos)
        if self.offset:
            return val
        else:
            return comma_format(val)


class PrettyDateFormatter(AutoDateFormatter):
    """This class provides a formatter which conforms to the
    desired date formats for the Phedex system.
    """

    def __init__(self, locator):
        """Format dates according to the Phedex system"""
        tz = pytz.timezone("UTC")
        AutoDateFormatter.__init__(self, locator, tz=tz)

    def __call__(self, x, pos=0):
        scale = float(self._locator._get_unit())
        if scale == 365.0:
            self._formatter = DateFormatter("%Y", self._tz)
        elif scale == 30.0:
            self._formatter = DateFormatter("%b %Y", self._tz)
        elif (scale >= 1.0) and (scale <= 7.0):
            self._formatter = DateFormatter("%Y-%m-%d", self._tz)
        elif scale == (1.0 / 24.0):
            self._formatter = DateFormatter("%H:%M", self._tz)
        elif scale == (1.0 / (24 * 60)):
            self._formatter = DateFormatter("%H:%M", self._tz)
        elif scale == (1.0 / (24 * 3600)):
            self._formatter = DateFormatter("%H:%M:%S", self._tz)
        else:
            self._formatter = DateFormatter("%b %d %Y %H:%M:%S", self._tz)

        return self._formatter(x, pos)


class PrettyDateLocator(AutoDateLocator):
    def get_locator(self, dmin, dmax):
        "pick the best locator based on a distance"

        delta = relativedelta(dmax, dmin)
        numYears = delta.years * 1.0
        numMonths = (numYears * 12.0) + delta.months
        numDays = (numMonths * 31.0) + delta.days
        numHours = (numDays * 24.0) + delta.hours
        numMinutes = (numHours * 60.0) + delta.minutes
        numSeconds = (numMinutes * 60.0) + delta.seconds

        numticks = 5

        # self._freq = YEARLY
        interval = 1
        bymonth = 1
        bymonthday = 1
        byhour = 0
        byminute = 0
        bysecond = 0

        if numYears >= numticks:
            self._freq = YEARLY
        elif numMonths >= numticks:
            self._freq = MONTHLY
            bymonth = list(range(1, 13))
            if (0 <= numMonths) and (numMonths <= 14):
                interval = 1  # show every month
            elif (15 <= numMonths) and (numMonths <= 29):
                interval = 3  # show every 3 months
            elif (30 <= numMonths) and (numMonths <= 44):
                interval = 4  # show every 4 months
            else:  # 45 <= numMonths <= 59
                interval = 6  # show every 6 months
        elif numDays >= numticks:
            self._freq = DAILY
            bymonth = None
            bymonthday = list(range(1, 32))
            if (0 <= numDays) and (numDays <= 9):
                interval = 1  # show every day
            elif (10 <= numDays) and (numDays <= 19):
                interval = 2  # show every 2 days
            elif (20 <= numDays) and (numDays <= 35):
                interval = 3  # show every 3 days
            elif (36 <= numDays) and (numDays <= 80):
                interval = 7  # show every 1 week
            else:  # 100 <= numDays <= ~150
                interval = 14  # show every 2 weeks
        elif numHours >= numticks:
            self._freq = HOURLY
            bymonth = None
            bymonthday = None
            byhour = list(range(0, 24))  # show every hour
            if (0 <= numHours) and (numHours <= 14):
                interval = 1  # show every hour
            elif (15 <= numHours) and (numHours <= 30):
                interval = 2  # show every 2 hours
            elif (30 <= numHours) and (numHours <= 45):
                interval = 3  # show every 3 hours
            elif (45 <= numHours) and (numHours <= 68):
                interval = 4  # show every 4 hours
            elif (68 <= numHours) and (numHours <= 90):
                interval = 6  # show every 6 hours
            else:  # 90 <= numHours <= 120
                interval = 12  # show every 12 hours
        elif numMinutes >= numticks:
            self._freq = MINUTELY
            bymonth = None
            bymonthday = None
            byhour = None
            byminute = list(range(0, 60))
            if numMinutes > (10.0 * numticks):
                interval = 10
            # end if
        elif numSeconds >= numticks:
            self._freq = SECONDLY
            bymonth = None
            bymonthday = None
            byhour = None
            byminute = None
            bysecond = list(range(0, 60))
            if numSeconds > (10.0 * numticks):
                interval = 10
            # end if
        else:
            # do what?
            #   microseconds as floats, but floats from what reference point?
            pass

        rrule = rrulewrapper(
            self._freq,
            interval=interval,
            dtstart=dmin,
            until=dmax,
            bymonth=bymonth,
            bymonthday=bymonthday,
            byhour=byhour,
            byminute=byminute,
            bysecond=bysecond,
        )

        locator = RRuleLocator(rrule, self.tz)
        locator.set_axis(self.axis)

        return locator


def pretty_float(num):
    if num > 1000:
        return comma_format(int(num))

    try:
        floats = int(max(2 - max(numpy.floor(numpy.log(abs(num) + 1e-3) / numpy.log(10.0)), 0), 0))
    except Exception:
        floats = 2
    format = "%." + str(floats) + "f"
    if isinstance(num, tuple):
        return format % float(num[0])
    else:
        try:
            retval = format % float(num)
        except Exception:
            raise Exception(f"Unable to convert {str(num)} into a float.")
        return retval


def statistics(results, span=None, is_timestamp=False):
    results = dict(results)
    if span is not None:
        parsed_data = {}
        min_key = min(results)
        max_key = max(results)
        for i in range(min_key, max_key + span, span):
            if i in results:
                parsed_data[i] = results[i]
                del results[i]
            else:
                parsed_data[i] = 0.0
        if len(results) > 0:
            raise Exception("Unable to use all the values for the statistics")
    else:
        parsed_data = results
    values = list(parsed_data.values())
    data_min = min(values)
    data_max = max(values)
    data_avg = numpy.average(values)
    if is_timestamp:
        current_time = max(parsed_data)
        data_current = parsed_data[current_time]
        return data_min, data_max, data_avg, data_current
    else:
        return data_min, data_max, data_avg


def makeDataFromCSV(csv):
    """Generate plot data dictionary from a csv file or string"""

    if os.path.exists(csv):
        with open(csv) as fdata:
            flines = fdata.readlines()
    else:
        flines = csv.split("\n")

    graph_data = {}
    labels = flines[0].strip().split(",")
    if len(labels) == 2:
        # simple plot data
        for line in flines:
            line = line.strip()
            if line[0] != "#":
                key, value = line.split(",")
                graph_data[key] = value

    elif len(flines) == 2:
        values = flines[1].strip().split(",")
        for key, value in zip(labels, values):
            graph_data[key] = value

    elif len(labels) > 2:
        # stacked graph data
        del labels[0]
        del flines[0]
        for label in labels:
            plot_data = {}
            index = labels.index(label) + 1
            for line in flines:
                values = line.strip().split(",")
                value = values[index].strip()
                # if value:
                plot_data[values[0]] = values[index]
                # else:
                # plot_data[values[0]] = '0.'
                # pass
            graph_data[label] = dict(plot_data)

    return graph_data


def darkenColor(color, factor=2):
    c1 = int(color[1:3], 16)
    c2 = int(color[3:5], 16)
    c3 = int(color[5:7], 16)

    c1 /= factor
    c2 /= factor
    c3 /= factor

    result = "#" + (
        str(hex(c1)).replace("0x", "").zfill(2)
        + str(hex(c2)).replace("0x", "").zfill(2)
        + str(hex(c3)).replace("0x", "").zfill(2)
    )
    return result
