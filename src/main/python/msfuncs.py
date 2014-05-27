#!/usr/bin/python
#
# author stephenb
#
from logger import *
from io_funcs import *
from misc_funcs import format_now, now
import process_funcs

class MsFuncs:

  def __init__(self, env, props):
    import os
    opsdir=props.get("ms.ops.dir",'/mnt/ms')
    if not os.path.exists(opsdir):
      cmd="sudo mkdir -p %s && sudo chown %s.%s %s" %(opsdir, os.environ['USER'],os.environ['USER'],opsdir)
      # cmd="sudo mkdir -p %s && sudo chown %s.%s %s" %(opsdir, os.environ['USER'],os.environ['USER'],opsdir)
      tracerr("Error: you must create the ms.ops.dir using the following command: %s" %cmd)
      sys.exit(1)
      # process_funcs.shellexec("sudo mkdir -p %s && sudo chown %s.%s %s" %(opsdir, os.environ['USER'],os.environ['USER'],opsdir))
    self.env = env
    self.props = props
    self.alert_exec=self.props.get("alert.command")
    self.alertfile=self.props.get("alert.output.file","/mnt/ms/alerts.log")
    from process_funcs import set_alert_func
    set_alert_func(self.alert)
    self.stdout = ""
    self.stderr = ""
    self.hive_vars_written = False

  def hivevars(self):
    hvars = r"""
  set hive.exec.mode.local.auto=true;
  set hive.exec.script.maxerrsize=100000000 ;
  """
    return hvars

  def hive_vars_file(self):
    return self.props.get("hive-vars-file", "/mnt/ms/hiveVars.q")

  def alert_log(self):
    return self.props.get("alerts-log", "/mnt/ms/alerts.log")
  # 
  # def alert_error_logname(self):
  #   return self.props.get("alerts-error-log", "/mnt/ms/alerts-error.log")

  def alert_log(self, msg):
    logfile = self.props.get("alerts-log", "/mnt/ms/alerts.log")
    with open(logfile, "a") as f:
      f.write("[%s] %s" % (format_now(), msg))

  def alert_error_log(self, msg):
    logfile = self.props.get("alerts-error-log", "/mnt/ms/alerts-error.log")
    with open(logfile, "a") as f:
      f.write("[%s] %s" % (format_now(), msg))

  def alertinfo(self,msg) :
    from process_funcs import shellexec
    info( "ALERT: %s\n" %(msg))
    with open(self.alertfile, 'a') as f:
      f.write(msg)
    if not self.props.get("stub_bash_call",False):
      shellexec(self.alert_exec)

  # Read stdout/stderr from subprocess
  def alert(self, err):
    tracerr(err)
    self.alert_error_log(err)

  # Invokes a shell command and bookends it with timing info and Alerts 
  # Alert notifications
  def run_bash_step(self, cmd_name, cmd, stub_call=False) :
    from misc_funcs import now
    from misc_funcs import seconds_since
    from process_funcs import shellexec_captures
    start=now()
    self.alertinfo("Starting Step %s" %(cmd_name))
    debug("Running command %s" %(cmd))
    if stub_call or self.props.get("stub_bash_call","false").lower() == "true":
      output = "Stub OUTPUT for %s" % (cmd)
      err = "Stub ERROR for %s" %cmd_name
    else:
      output, err = shellexec_captures(cmd)
    if err:
      self.alert_error_log("ERROR running command=[%s]: [%s]" %(cmd_name,err))
    else:
      self.alert_log("For %s command=[%s] completed at %s with duration=%d secs.\n" %(cmd_name,cmd,start, seconds_since(start)))
    
    self.alertinfo("Completed Step %s duration=%f secs" %(cmd_name, seconds_since(start)))
    return output,err

  def tmpdir(self):
    return self.props.get("tmpdir","/tmp/ms")

  def hive_exec(self):
    return self.props.get("hive_exec","/bin/hive -v")

  def hive_vars_file(self):
    return self.props.get("hive_vars_file", "/tmp/ms/hiveVars.q")

  def run_hive_step(self, cmd_name, hive_cmd, stub_call=False):
    from misc_funcs import format_now
    import os
    # if not self.hive_vars_written:
    tmpdir=self.hive_vars_file()[0:self.hive_vars_file().rfind('/')]
    if not os.path.exists(tmpdir):
      os.mkdir(tmpdir,0777)
    with open(self.hive_vars_file(), "w") as f:
      f.write(self.hivevars())
    # self.hive_vars_written = True

    hive_tmp_file="%s/hivems.%s.q" %(self.tmpdir(),format_now())
    mkdirs(hive_tmp_file[0:hive_tmp_file.rfind('/')])
    writefile(hive_tmp_file,hive_cmd.rstrip())
    info("Running hive command=%s from file=%s%s" %(cmd_name, hive_tmp_file,
         " (STUB=true)" if stub_call else ""))
    hive_bash_cmd="%s -i %s -f %s" %(self.hive_exec(),self.hive_vars_file(), hive_tmp_file)
    output,err=self.run_bash_step(cmd_name, hive_bash_cmd, stub_call)
    self.write_outputs(output, err)
    tracerr("done running hive step err is [%s]" %err)
    if err:
      tracerr(err,False)
    info(output)
    return output,err

  # Run a hive query with given tag and sql
  def run_hive_query(self, cmd_name,hive_cmd):
    info("Running hive query=%s" %(hive_cmd))
    hive_bash_cmd="%s -e \"%s\"" %(self.hive_exec, hive_cmd)
    output,err=self.run_bash_step(cmd_name, hive_bash_cmd).rstrip()
    self.append_outputs(output, err)
    if err:
      tracerr(err,False)
    info(output)
    return output

  def append_outputs(self, output, err):
    self.stdout += output
    self.stderr += err

  def write_stdout(self, output):
    self.stdout += output + "\n"

  def write_stderr(self, err):
    self.stderr += err + "\n"

  def write_outputs(self, output, err):
    self.write_stdout(output)
    self.write_stderr(err)

  # def write_outputs(self):
  #   curtime = format_now()
  #   stdout_file="%s/ms.%s.stdout" %(self.tmpdir(),curtime)
  #   writefile(stdout_file, self.stdout)
  #   stderr_file="%s/ms.%s.stderr" %(self.tmpdir(),curtime)
  #   writefile(stderr_file, self.stderr)

  def check_hdfs_dir(self,hdfsdir, create=False):
    cmd = "hdfs dfs -ls %s" %hdfsdir
    stdout, stderr = self.run_bash_step("check if hdfs dir exists for %s" %hdfsdir,cmd)
    if "No such file" in stderr:
      if create:
        debug("Creating directory %s" %hdfsdir)
        self.create_hdfs_dir(hdfsdir)
        return True
      return False
    else:
      return True

  def create_hdfs_dir(self, hdfsdir):
    cmd = "hdfs dfs -mkdir %s" % hdfsdir
    stdout, stderr = self.run_bash_step("Create hdfs dir %s" % hdfsdir, cmd)
    print stdout
    if "xception" in stderr:
      tracerr("create_hdfs_dir failed",stderr)
