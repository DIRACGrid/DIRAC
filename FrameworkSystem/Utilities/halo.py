# -*- coding: utf-8 -*-
# pylint: disable=unsubscriptable-object
""" Changed for DIRAC. Beautiful terminal spinners in Python. Source: https://github.com/manrajgrover/halo and dependens

    MIT License

    Copyright (c) 2017 Manraj Singh

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from __future__ import absolute_import, unicode_literals

import os
import re
import sys
import six
import time
import ctypes
import atexit
import signal
import codecs
import platform
import functools
import threading
try:
  from shutil import get_terminal_size
except ImportError:
  from backports.shutil_get_terminal_size import get_terminal_size


def coloredFrame(text, color=None, onColor=None, attrs=['bold']):
  """ Colorize text, while stripping nested ANSI color sequences.
      Source: https://github.com/hfeeki/termcolor/blob/master/termcolor.py

      :param str text: text
      :param str color: text colors -> red, green, yellow, blue, magenta, cyan, white.
      :param str onColor: text highlights -> on_red, on_green, on_yellow, on_blue, on_magenta, on_cyan, on_white.
      :param list attrs: attributes -> bold, dark, underline, blink, reverse, concealed.

      --
          coloredFrame('Hello, World!', 'red', 'on_grey', ['blue', 'blink'])
          coloredFrame('Hello, World!', 'green')

      :return: str
  """
  ATTRIBUTES = dict(list(zip(['bold', 'dark', '', 'underline', 'blink', '', 'reverse', 'concealed'],
                             list(range(1, 9)))))
  del ATTRIBUTES['']
  ATTRIBUTES_RE = r'\033\[(?:%s)m' % '|'.join(['%d' % v for v in ATTRIBUTES.values()])
  HIGHLIGHTS = dict(list(zip(['on_grey', 'on_red', 'on_green', 'on_yellow', 'on_blue',
                              'on_magenta', 'on_cyan', 'on_white'], list(range(40, 48)))))
  HIGHLIGHTS_RE = r'\033\[(?:%s)m' % '|'.join(['%d' % v for v in HIGHLIGHTS.values()])
  COLORS = dict(list(zip(['grey', 'red', 'green', 'yellow', 'blue', 'magenta', 'cyan', 'white', ],
                         list(range(30, 38)))))
  COLORS_RE = r'\033\[(?:%s)m' % '|'.join(['%d' % v for v in COLORS.values()])
  RESET = '\033[0m'
  RESET_RE = r'\033\[0m'

  if os.getenv('ANSI_COLORS_DISABLED') is None:
    fmtStr = '\033[%dm%s'
    if color is not None:
      text = re.sub(COLORS_RE + '(.*?)' + RESET_RE, r'\1', text)
      text = fmtStr % (COLORS[color], text)
    if onColor is not None:
      text = re.sub(HIGHLIGHTS_RE + '(.*?)' + RESET_RE, r'\1', text)
      text = fmtStr % (HIGHLIGHTS[onColor], text)
    if attrs is not None:
      text = re.sub(ATTRIBUTES_RE + '(.*?)' + RESET_RE, r'\1', text)
      for attr in attrs:
        text = fmtStr % (ATTRIBUTES[attr], text)
    return text + RESET
  else:
    return text


class StreamWrapper(object):
  """ Wraps a stream (such as stdout), acting as a transparent proxy for all
      attribute access apart from method 'write()', which is delegated to our
      Converter instance.
      Source: https://github.com/tartley/colorama
  """

  def __init__(self, wrapped, converter):
    # double-underscore everything to prevent clashes with names of
    # attributes on the wrapped stream object.
    self.__wrapped = wrapped
    self.__convertor = converter

  def __getattr__(self, name):
    return getattr(self.__wrapped, name)

  def __enter__(self, *args, **kwargs):
    # special method lookup bypasses __getattr__/__getattribute__, see
    # https://stackoverflow.com/questions/12632894/why-doesnt-getattr-work-with-exit
    # thus, contextlib magic methods are not proxied via __getattr__
    return self.__wrapped.__enter__(*args, **kwargs)

  def __exit__(self, *args, **kwargs):
    return self.__wrapped.__exit__(*args, **kwargs)

  def write(self, text):
    self.__convertor.write(text)

  def isatty(self):
    stream = self.__wrapped
    if 'PYCHARM_HOSTED' in os.environ:
      if stream is not None and (stream is sys.__stdout__ or stream is sys.__stderr__):
        return True
    try:
      streamIsATTY = stream.isatty
    except AttributeError:
      return False
    else:
      return streamIsATTY()

  @property
  def closed(self):
    stream = self.__wrapped
    try:
      return stream.closed
    except AttributeError:
      return True


class PreWrapp(object):
  """ Source: https://github.com/tartley/colorama
  """

  def __init__(self, wrapped):
    if os.name == 'nt':
      raise BaseException('Not support')
    # The wrapped stream (normally sys.stdout or sys.stderr)
    self.wrapped = wrapped
    # create the proxy wrapping our output stream
    self.stream = StreamWrapper(wrapped, self)

  def write(self, text):
    self.wrapped.write(text)
    self.wrapped.flush()
    self.resetAll()

  def resetAll(self):
    if not self.stream.closed:
      self.wrapped.write('\033[0m')


def resetAll():
  if PreWrapp is not None:  # Issue #74: objects might become None at exit
    PreWrapp(sys.stdout).resetAll()


sys.stdout = PreWrapp(sys.stdout).stream
sys.stderr = PreWrapp(sys.stderr).stream
atexit.register(resetAll)


def isSupported():
  """ Check whether operating system supports main symbols or not.

      :return: boolen -- Whether operating system supports main symbols or not
  """
  return platform.system() != 'Windows'


def getEnvironment():
  """ Get the environment in which halo is running

      :return: str -- Environment name
  """
  try:
    from IPython import get_ipython
  except ImportError:
    return 'terminal'
  try:
    shell = get_ipython().__class__.__name__
    if shell == 'ZMQInteractiveShell':  # Jupyter notebook or qtconsole
      return 'jupyter'
    elif shell == 'TerminalInteractiveShell':  # Terminal running IPython
      return 'ipython'
    else:
      return 'terminal'  # Other type (?)
  except NameError:
    return 'terminal'


def isTextType(text):
  """ Check if given parameter is a string or not

      :param str text: Parameter to be checked for text type

      :return: boolen -- Whether parameter is a string or not
  """
  return bool(isinstance(text, six.text_type) or isinstance(text, six.string_types))


def decodeUTF8Text(text):
  """ Decode the text from utf-8 format

      :param str text: String to be decoded

      :return: str -- Decoded string
  """
  try:
    return codecs.decode(text, 'utf-8')
  except (TypeError, ValueError):
    return text


def encodeUTF8Text(text):
  """ Encodes the text to utf-8 format

      :param str text: String to be encoded

      :return: str -- Encoded string
  """
  try:
    return codecs.encode(text, 'utf-8', 'ignore')
  except (TypeError, ValueError):
    return text


def getTerminalColumns():
  """ Determine the amount of available columns in the terminal

      :return: int -- Terminal width
  """
  # If column size is 0 either we are not connected
  # to a terminal or something else went wrong. Fallback to 80.
  return 80 if get_terminal_size().columns == 0 else get_terminal_size().columns


class Halo(object):
  """ Halo library.

      CLEAR_LINE -- Code to clear the line
  """
  class Done(Exception):
    """ Done exception """
    pass

  class CursorInfo(ctypes.Structure):
    # Need for cursor
    if os.name == 'nt':
      _fields_ = [("size", ctypes.c_int), ("visible", ctypes.c_byte)]

  CLEAR_LINE = '\033[K'
  SPINNER_PLACEMENTS = ('left', 'right',)

  def __init__(self, text='', color='green', textColor=None, spinner=None,
               animation=None, placement='left', interval=-1, enabled=True, stream=sys.stdout, result='succeed'):
    """ Constructs the Halo object.

        :param str text: Text to display.
        :param str color: Color of the text.
        :param str textColor: Color of the text to display.
        :param str,dict spinner: String or dictionary representing spinner.
        :param basesrting animation: Animation to apply if text is too large. Can be one of `bounce`, `marquee`.
               Defaults to ellipses.
        :param str placement: Side of the text to place the spinner on. Can be `left` or `right`.
               Defaults to `left`.
        :param int interval: Interval between each frame of the spinner in milliseconds.
        :param boolean enabled: Spinner enabled or not.
        :param io stream: IO output.
    """
    self._newline = None
    self._result = result
    self._color = color
    self._animation = animation
    self.spinner = spinner
    self.text = text
    self._textColor = textColor
    self._interval = int(interval) if int(interval) > 0 else self._spinner['interval']
    self._stream = stream
    self.placement = placement
    self._frameIndex = 0
    self._textIndex = 0
    self._spinnerThread = None
    self._stopSpinner = None
    self._spinnerId = None
    self.enabled = enabled
    environment = getEnvironment()

    def cleanUp():
      """ Handle cell execution"""
      self.__stop()

    if environment in ('ipython', 'jupyter'):
      from IPython import get_ipython
      ip = get_ipython()
      ip.events.register('post_run_cell', cleanUp)
    else:  # default terminal
      atexit.register(cleanUp)

  def __enter__(self):
    """ Starts the spinner on a separate thread. For use in context managers.
    """
    return self.start()

  def __exit__(self, eType, eValue, traceback):
    """ Stops the spinner. For use in context managers."""
    if eType:
      self._newline = False
      self._text['original'] = ''
      if isinstance(eValue, SystemExit) and eValue.code in [None, 0]:
        self.succeed()
      else:
        self.fail()
    elif self._result == 'succeed':
      self.succeed()
    elif self._result == 'warn':
      self.warn()
    elif self._result == 'info':
      self.info()
    else:
      self.stop()

  def __call__(self, f):
    """ Allow the Halo object to be used as a regular function decorator.
    """
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
      with self:
        return f(*args, **kwargs)
    return wrapped

  @property
  def spinner(self):
    """ Getter for spinner property.

        :return: dict -- spinner value
    """
    return self._spinner

  @spinner.setter
  def spinner(self, spinner=None):
    """ Setter for spinner property.

        :param dict,str spinner: Defines the spinner value with frame and interval
    """
    self._spinner = {"interval": 80, "frames": ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]}
    self._frameIndex = 0
    self._textIndex = 0

  @property
  def text(self):
    """ Getter for text property.

        :return: str -- text value
    """
    return self._text['original']

  @text.setter
  def text(self, text):
    """ Setter for text property.

        :param str text: Defines the text value for spinner
    """
    self._text = self._getText(text)

  @property
  def result(self):
    """ Getter for result property.

        :return: str -- result value
    """
    return self._result

  # pylint: disable=function-redefined
  @text.setter
  def result(self, result):
    """ Setter for result property.

        :param str result: Defines the result of with
    """
    self._result = result

  @property
  def textColor(self):
    """ Getter for text color property.

        :return: str -- text color value
    """
    return self._textColor

  @textColor.setter
  def textColor(self, textColor):
    """ Setter for text color property.

        :param str textColor: Defines the text color value for spinner
    """
    self._textColor = textColor

  @property
  def color(self):
    """ Getter for color property.

        :return: str -- color value
    """
    return self._color

  @color.setter
  def color(self, color):
    """ Setter for color property.

        :param str color: Defines the color value for spinner
    """
    self._color = color

  @property
  def placement(self):
    """ Getter for placement property.

        :return: str -- spinner placement
    """
    return self._placement

  @placement.setter
  def placement(self, placement):
    """ Setter for placement property.

        :param str placement: Defines the placement of the spinner
    """
    if placement not in self.SPINNER_PLACEMENTS:
      raise ValueError("Unknown spinner placement '{0}', available are {1}".format(placement, self.SPINNER_PLACEMENTS))
    self._placement = placement

  @property
  def spinner_id(self):
    """ Getter for spinner id

        :return: str -- Spinner id value
    """
    return self._spinnerId

  @property
  def animation(self):
    """ Getter for animation property.

        :return: str -- Spinner animation
    """
    return self._animation

  @animation.setter
  def animation(self, animation):
    """ Setter for animation property.

        :param str animation: Defines the animation of the spinner
    """
    self._animation = animation
    self._text = self._getText(self._text['original'])

  def _checkStream(self):
    """ Returns whether the stream is open, and if applicable, writable

        :return: bool -- Whether the stream is open
    """
    if self._stream.closed:
      return False
    try:
      # Attribute access kept separate from invocation, to avoid
      # swallowing AttributeErrors from the call which should bubble up.
      checkStreamWritable = self._stream.writable
    except AttributeError:
      pass
    else:
      return checkStreamWritable()
    return True

  def _write(self, s):
    """ Write to the stream, if writable

        :params str s: Characters to write to the stream
    """
    if self._checkStream():
      self._stream.write(s)

  def _hideCursor(self):
    """ Disable the user's blinking cursor
    """
    if self._checkStream() and self._stream.isatty():
      for sid in [signal.SIGINT, signal.SIGTSTP]:
        signal.signal(sid, self._showCursor)
      if os.name == 'nt':
        ci = CursorInfo()  # pylint: disable=undefined-variable
        handle = ctypes.windll.kernel32.GetStdHandle(-11)
        ctypes.windll.kernel32.GetConsoleCursorInfo(handle, ctypes.byref(ci))
        ci.visible = False
        ctypes.windll.kernel32.SetConsoleCursorInfo(handle, ctypes.byref(ci))
      elif os.name == 'posix':
        sys.stdout.write("\033[?25l")
        sys.stdout.flush()

  def _showCursor(self, *args):
    """ Re-enable the user's blinking cursor
    """
    if self._checkStream() and self._stream.isatty():
      if os.name == 'nt':
        ci = CursorInfo()  # pylint: disable=undefined-variable
        handle = ctypes.windll.kernel32.GetStdHandle(-11)
        ctypes.windll.kernel32.GetConsoleCursorInfo(handle, ctypes.byref(ci))
        ci.visible = True
        ctypes.windll.kernel32.SetConsoleCursorInfo(handle, ctypes.byref(ci))
      elif os.name == 'posix':
        sys.stdout.write("\033[?25h")
        sys.stdout.flush()
    if args:
      raise SystemExit(args[0])

  def _getText(self, text):
    """ Creates frames based on the selected animation

        :params str text: text
    """
    animation = self._animation
    strippedText = text.strip()

    # Check which frame of the animation is the widest
    maxSpinnerLength = max([len(i) for i in self._spinner['frames']])

    # Subtract to the current terminal size the max spinner length
    # (-1 to leave room for the extra space between spinner and text)
    terminalWidth = getTerminalColumns() - maxSpinnerLength - 1
    textLength = len(strippedText)
    frames = []
    if terminalWidth < textLength and animation:

      if animation == 'bounce':
        # Make the text bounce back and forth
        for x in range(0, textLength - terminalWidth + 1):
          frames.append(strippedText[x:terminalWidth + x])
        frames.extend(list(reversed(frames)))

      elif 'marquee':
        # Make the text scroll like a marquee
        strippedText = strippedText + ' ' + strippedText[:terminalWidth]
        for x in range(0, textLength + 1):
          frames.append(strippedText[x:terminalWidth + x])

    elif terminalWidth < textLength and not animation:
      # Add ellipsis if text is larger than terminal width and no animation was specified
      frames = [strippedText[:terminalWidth - 4] + '... ']
    else:
      frames = [strippedText]
    return {'original': text, 'frames': frames}

  def clear(self):
    """ Clears the line and returns cursor to the start.
    """
    self._write('\r')
    self._write(self.CLEAR_LINE)
    return self

  def _renderFrame(self):
    """ Renders the frame on the line after clearing it.
    """
    if not self.enabled:
      # in case we're disabled or stream is closed while still rendering,
      # we render the frame and increment the frame index, so the proper
      # frame is rendered if we're reenabled or the stream opens again.
      return
    self.clear()
    frame = self.frame()
    output = '\r{}'.format(frame)
    try:
      self._write(output)
    except UnicodeEncodeError:
      self._write(encodeUTF8Text(output))

  def render(self):
    """ Runs the render until thread flag is set.
    """
    while not self._stopSpinner.is_set():
      self._renderFrame()
      time.sleep(0.001 * self._interval)
    return self

  def frame(self):
    """ Builds and returns the frame to be rendered
    """
    frames = self._spinner['frames']
    frame = frames[self._frameIndex]
    if self._color:
      frame = coloredFrame(frame, self._color)
    self._frameIndex += 1
    self._frameIndex = self._frameIndex % len(frames)
    textFrame = self.textFrame()
    return u'{0} {1}'.format(*[(textFrame, frame) if self._placement == 'right' else (frame, textFrame)][0])

  def textFrame(self):
    """ Builds and returns the text frame to be rendered
    """
    if len(self._text['frames']) == 1:
      if self._textColor:
        return coloredFrame(self._text['frames'][0], self._textColor)
      # Return first frame (can't return original text because at this point it might be ellipsed)
      return self._text['frames'][0]
    frames = self._text['frames']
    frame = frames[self._textIndex]
    self._textIndex += 1
    self._textIndex = self._textIndex % len(frames)
    return coloredFrame(frame, self._textColor) if self._textColor else frame

  def start(self, text=None):
    """ Starts the spinner on a separate thread.

        :param str text: Text to be used alongside spinner
    """
    if text is not None:
      self.text = text
    if self._spinnerId is not None:
      return self
    if not (self.enabled and self._checkStream()):
      return self
    self._hideCursor()
    self._stopSpinner = threading.Event()
    self._spinnerThread = threading.Thread(target=self.render)
    self._spinnerThread.setDaemon(True)
    self._renderFrame()
    self._spinnerId = self._spinnerThread.name
    self._spinnerThread.start()
    return self

  def __stop(self):
    if self._spinnerThread and self._spinnerThread.is_alive():
      self._stopSpinner.set()
      self._spinnerThread.join()

    if self.enabled:
      self.clear()

    self._frameIndex = 0
    self._spinnerId = None
    self._showCursor()
    return self

  def succeed(self, text=None):
    """ Shows and persists success symbol and text and exits.

        :param str text: Text to be shown alongside success symbol.
    """
    self._color = 'green'
    return self.stop(symbol='✔', text=text)

  def fail(self, text=None):
    """ Shows and persists fail symbol and text and exits.

        :param str text: Text to be shown alongside fail symbol.
    """
    self._color = 'red'
    return self.stop(symbol='✖', text=text)

  def warn(self, text=None):
    """ Shows and persists warn symbol and text and exits.

        :param str text: Text to be shown alongside warn symbol.
    """
    self._color = 'yellow'
    return self.stop(symbol='⚠', text=text)

  def info(self, text=None):
    """ Shows and persists info symbol and text and exits.

        :param str text: Text to be shown alongside info symbol.
    """
    self._color = 'blue'
    return self.stop(symbol='ℹ', text=text)

  def stop(self, text=None, symbol=None):
    """ Stops the spinner and persists the final frame to be shown.

        :param str text: Text to be shown in final frame
        :param str symbol: Symbol to be shown in final frame
    """
    if not (symbol and text):
      self.__stop()
    if not self.enabled:
      return self
    self.__stop()
    symbol = decodeUTF8Text(symbol) if symbol is not None else ''
    text = decodeUTF8Text(text) if text is not None else self._text['original']
    symbol = coloredFrame(symbol, self._color) if self._color and symbol else symbol
    text = coloredFrame(text, self._textColor) if self._textColor and text else text.strip()
    output = u'{0} {1}'.format(*[(text, symbol) if self._placement == 'right' else (symbol, text)][0])
    output += '' if self._newline is False else '\n'
    try:
      self._write(output)
    except UnicodeEncodeError:
      self._write(encodeUTF8Text(output))
    return self
