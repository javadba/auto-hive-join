#!/usr/bin/python
#
# process_funcs.py:  Process/subprocess related Helper methods
# author stephenb
#
# Process related helper methods
#
import subprocess
import multiprocessing as mp
from misc_funcs import *

class StderrWriter(object):
  import sys
  def __init__(self, fname, mode):
    self.file = open(fname, mode)
    self.stderr = sys.stderr
    sys.stderr = self
  def write(self, data):
    self.file.write(data)
    self.stderr.write(data)
  def __del__(self):
    self.file.close()
    sys.stderr = self.stderr

class Tee(object):
  import sys
  def __init__(self, fname, mode):
    self.file = open(fname, mode)
    self.stderr_writer = StderrWriter(fname+".stderr", mode)
    sys.stderr=self.stderr_writer.stderr
    self.stdout = sys.stdout
    sys.stdout = self
  def __enter__(self):
    return self
  def __exit__(self, exc_type, exc_val, exc_tb):
  #   self.__del__()
  # def __del__(self):
    sys.stdout = self.stdout
    sys.stderr = self.stderr_writer.stderr
    self.file.close()
    del self.stderr_writer
  def write(self, data):
    self.file.write(data)
    self.stdout.write(data)
    self.stderr_writer.write(data)

# set up alerting. This is done within the classifier_funcs and connected to
# papertrail/scout
def alert_default(msg):
  tracerr(msg)

__alert = alert_default

# Alert function needs to be set .. or it just dumps to stderr
def set_alert_func(f):
  global __alert
  __alert = f

from io_funcs import pwd
ignore_errs_file="%s/ignore_errs.txt" %pwd()

from io_funcs import readfile
ignore_errs = filter(lambda e: e.strip(), readfile(ignore_errs_file).split("\\n"))

def ignore_err(errmsg):
  if not errmsg:
    return True
  else:
    errmsg = errmsg.lower()
    for err in ignore_errs:
      if err.lower() in errmsg:
        return True
    return False

# Read stdout/stderr from a subprocess
def read_outputs(cmd, sp):
    out = ""; err = ""
    while True:
      tmpout, tmperr = (sp.stdout.read(), sp.stderr.read())
      if not ignore_err(tmperr):
        tracerr("Error invoking %s: %s" %(cmd, tmperr))
        err += tmperr
      if tmpout:
        out += tmpout
      if not tmpout and not tmperr:
        break   # Believe it or not this is the canonical way to mimic "do .. while (x)" in python
                # http://stackoverflow.com/questions/1662161/is-there-a-do-until-in-python
    if err:
      __alert(err)
    rval = (out, err) if out is not None or err is not None else (None,None)
    return rval

# Execute and capture stdout/stderr from subprocess
def shellexec_captures(cmd):
    import subprocess
    sp = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    return read_outputs(cmd, sp)

# Execute subprocess
def shellexec(cmd):
  import subprocess
  sp = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
  return sp.communicate()

# Block on set of subprocesses
def waitall(subprocs):
  import os
  outstreams = [()]
  spmap = {}
  # for proc in subprocs:
  #   spmap[proc.pid] = proc
  # 
  # debug("spmap is %s" %spmap)
  # while spmap:
  #   debug("Waiting for %d processes..." % len(spmap))
  #   pid, status = os.wait()
  #   sp = spmap[pid]
  #   out,err = read_outputs("blah",sp)
  #   outstreams.append((out,err))
  #   if pid in spmap:
  #     del spmap[pid]
  return outstreams
  # return subprocsoutstreams

# Wrap a shell/exec call with instrumentation to capture stdout/stderr and to place results in a queue
from subprocess import *
def wrap_for_process(q, funcname, func, args=(), nretries=1):
  def check_retry(nretries):
    tracerr("Error invoking subprocess %s (%s) retries=%d" % (funcname, func, nretries), True)
    nretries -= 1
    if nretries < 0:
      raise
  while nretries >= 0:
    try:
      tracerr("Subprocess %s running %s(%s) type(args) is %s len=%d" %(funcname, func,printarr(args), type(args), len(args)))
      sp = subprocess.Popen(func,stdout=PIPE,stderr=PIPE,shell=True)
      # sp = subprocess.Popen(func,args, stdout=PIPE,stderr=PIPE,shell=True)
      out,err = sp.communicate()
      debug("out=[%s]  err=[%s]" %(out,err))
      q.put((funcname, (out,err)))
      debug("Subprocess %s running as pid %d" %(funcname, sp.pid))
      sp.wait()
      return (out,err)
    except:
      nretries = check_retry(nretries)

# Wrap a python function call with instrumentation to place results in a queue
def wrap_for_func(q, funcname, func, args=(), nretries=0):
  debug("wrap_for_func: args is %s" %(str(args) if args else "(none)"))

  def check_retry(nretries):
    tracerr("Error invoking function %s retries=%d" %(funcname,nretries),True)
    nretries -= 1
    if nretries < 0:
      raise
    return nretries

  while nretries>=0:
    try:
      # TODO: use Tee to capture stdout/stderr
      result = func(args) if args else func()
      q.put((funcname, result))
      return result
    except:
      nretries = check_retry(nretries)

# Invoke a set of procedure call definitions in parallel
# Input format is in a dict:
#   {procname, (function pointer or shell command, argument1, argument2, ..)}
#
def parallel(groupname, procinfos, stop_on_error=True):
  import multiprocessing.queues
  if not procinfos or len(procinfos)==0:
    tracerr("parallel: No procs submitted")
    return
  debug("procinfos: %s vals=%s" %(procinfos.items(), procinfos.values()))
  # print "f3shell procinfo: %s" %(printarr(procinfos['f3shell']))
  q = mp.queues.Queue()
  launches = {}
  for name, procinfo in procinfos.items():
    execFunc = procinfo[0] if is_iterable(procinfo) else procinfo
    args = procinfo[1:] if is_iterable(procinfo) else ()
    debug("procinfo=%s is_iterable(procinfo)=%s args=%s" %(procinfo,is_iterable(procinfo), args))
    # printarr("zip: %s" %(zip(range(len(procinfo)),procinfo)))
    debug("for %s execFunc=%s args=%s" %(name, execFunc, args))
    launches[name] = [wrap_for_func if is_func(execFunc) else wrap_for_process,
               q,
               name,
               execFunc,
               args]
               # procinfo[1:] if len(procinfo) > 1 else ())
  # debug("launch[0]=%s launch[1:]=%s Launches: %s" % ([launch[0][0] for key,launch in launches.items()], [launch[1:] for key,launch in launches.items()], printarr(launches)))
  procs = [mp.Process(name=key, target=launch[0],args=launch[1:])
           for key, launch in launches.items()]
  debug("procs: %s" %(printarr(procs)))
  def start(proc):
    proc.start()
    # print("run invoked on %s" %(proc.name))
    return proc.pid
  def wait(proc):
    proc.join()
    # print("join completed on %s" %(proc.name))
    return proc._popen.pid
  [start(proc) for proc in procs]
  [wait(proc) for proc in procs]
  # [q.put((proc.name,wait(proc))) for proc in procs]
  # results = {proc.name : read_file_outputs(proc.name, proc._popen) for proc in procs}
  results = {}
  while not q.empty():
    val = q.get()
    results[val[0]] = val[1:]
  # debug(str(results))
  # print("Results are %s keys are %s" %(results, results.keys()))
  check_results(groupname, results, stop_on_error)
  return results

# Check results of a set of processes/function calls
def check_results(phase_name, results, stop_on_error):
  failed = False
  for func, result in results.iteritems():
    debug("result is %s is_iterable=%s len(result)=%d" %(result, is_iterable(result), len(result)))
    if is_iterable(result) and len(result) > 1:
      stderr = result[1]
      if stderr and not ignore_err(stderr):
        __alert("ERROR detected in %s: [%s]" % (phase_name, stderr))
        failed = True
    else:
      from numbers import Number
      if isinstance(result, Number):
        if result:
          __alert("ERROR detected in %s: [%s]" % (phase_name, result))
          failed = True
      else:
        tracerr("Unsupported result: %s type=%s" %(result, type(result)))
        # failed = True
  if failed and stop_on_error:
    msg = "One or more errors occurred in phase %s: [%s]" % (phase_name, results)
    __alert(msg)
    raise Exception(msg)
  return failed
