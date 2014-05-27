#!/usr/bin/python
# logger.py:  Simple logger methods for the classifier workflow
from datetime import datetime
import pickle
import sys
import traceback

debug = True

def debug(msg):
  if debug:
    sys.stderr.write("[%s] DEBUG: %s\n" % (datetime.now(), msg))

def info(msg):
  sys.stderr.write("[%s] INFO: %s\n" % (datetime.now(), msg))

def tracerr(msg, printException=False):
  if printException:
    exc_type, exc_value, tb = sys.exc_info()
    # Save the traceback and attach it to the exception object
    exc_lines = traceback.format_exception(exc_type, exc_value, tb)
    exc_value = ''.join(exc_lines)
    # errmsg = "  %s" % (pickle.dumps(exc_value))
    errmsg = "  %s" %exc_value
  else:
    errmsg = ""
  sys.stderr.write("[%s] ERROR: %s%s\n" % (datetime.now(), msg, errmsg))
