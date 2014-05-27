#!/usr/bin/python
# from process_funcs import *
from misc_funcs import *
from msfuncs import *
from msmeta import *

def getdelim(delim):
  return "|" if delim == "pipe" else delim

def create_table(mstable, props):
  msfuncs = MsFuncs("DEV", props)

  def createcols(cols):
    cstr = ""
    for col, ctype in cols:
      if len(cstr) > 0:
        cstr += ", "
      cstr += "%s %s" % (col.lower(), ctype.lower())
    return cstr

  localpath=mstable.props.get('localpath')
  hdfspath=mstable.props['hdfspath']
  if localpath:
    msfuncs.check_hdfs_dir(hdfspath,True)
    msfuncs.run_bash_step("Copy local file to hdfs",
                          "hdfs dfs -copyFromLocal %s %s/" %(localpath,hdfspath))
  else:
    if not msfuncs.check_hdfs_dir(hdfspath):
      raise Exception("No local file supplied and hdfs dir also does not exist. Check the values in <metada-file>|Tables|localpath and <metada-file>|Tables|hdfspath")

  # check we can create file in the hdfsdir
  msfuncs.run_bash_step("Verify create/rm access to hdfs-dir",
                        "echo '123' > /tmp/123; hdfs dfs -copyFromLocal /tmp/123 %s; hdfs dfs -rm %s/123;" %(hdfspath,hdfspath))
  tabscript = r"""
    set hivevar:tabdir=%(tabdir)s;
    set hivevar:table=%(table)s;
    set hivevar:columns=%(columns)s;
    %(drop_etable)s;
    create external table e${table} (${columns}) row format delimited fields terminated by '%(input_delimiter)s' lines terminated by '\n' location '${tabdir}';
    %(drop_table)s;
    create table %(table)s like e${table};
    insert overwrite table ${table} select * from e${table};
    desc formatted e${table};
    set hivevar:table_sample=%(table)s_sample;
    drop table if exists ${table_sample};
    create table ${table_sample} as select * from ${table} limit 20;
    select count(1) as 'NumRowsIn${table}' from ${table};
    """
  # -- create table %(table)s (%(columns)s) row format delimited fields terminated by '%(input_delimiter)s' lines terminated by '\n';

  cols=createcols(mstable.columns)
  tabscript = interpolate(tabscript,
                          {'table': mstable.name,
                           'input_delimiter' : getdelim(mstable.props['input_delimiter'].strip()),
                           'columns': cols,
                           'tabdir': mstable.props['hdfspath'],
                           'drop_etable' : "drop table if exists e%s" %mstable.name if mstable.drop_if_exists() else ";",
                           'drop_table' : "drop table if exists %s" %mstable.name if mstable.drop_if_exists() else ";",
                           })
  msfuncs.run_hive_step("Create hive table %s" % mstable.name, tabscript)  

def initdirs():
  import os
  user=os.environ['USER']
  try:
    os.system("mkdir -p /tmp/ms  && chown %s.%s /tmp/ms" %(user,user))
  except:
    tracerr("err on making /tmp/ms")
  try:
    os.system("mkdir -p /mnt/ms && chown %s.%s /mnt/ms" %(user,user))
  except:
    tracerr("err on making /mnt/ms")

def create_tables(meta, props, createtabs):
  initdirs()
  createdtabs = []
  for tab in map(lambda t: t.strip(), createtabs):
    if not tab in meta.tables().keys():
      tracerr("Requested createtable %s not found in metadata (available=%s)" %(tab, meta.tables().keys()))
    else:
      mstab = meta.tables().get(tab)
      create_table(mstab, props)
      createdtabs.extend(tab)
  return createdtabs

def run_join(join, allprops):
  msfuncs = MsFuncs("DEV", allprops)
  jscript = join.sql
  outtab=join.props['output_table']
  jointab_script="""
  drop table if exists %(jointab)s;
  create table %(jointab)s row format delimited fields terminated by '%(delimiter)s' as %(jscript)s;
  create external table e%(jointab)s like %(jointab)s location %(location)s;
  insert overwrite table e%(jointab)s select * from %(jointab)s;
  desc formatted %(jointab)s;
  """
  jointab_script = interpolate(jointab_script,
                               {'jointab' : outtab,
                                'delimiter' : getdelim(join.props['output_delimiter']),
                                'jscript' : jscript,
                                'location' : join.props['output_dir']
                               })
  msfuncs.run_hive_step("run join %s" % join.name, jointab_script)
  msfuncs.run_hive_step("Count records in %s" % join.props["output_table"],
    "select count(1) as 'NumRowsInJoin' from e%s" %join.props['output_table'])

def run_joins(meta, props, jointabs):
  joins = meta.joins().values()
  joined_tabs=[]
  for join in map(lambda j : j.strip(),jointabs):
    if not join in meta.joins().keys():
      tracerr("Requested join table %s not found in metadata available tables (%s)" %(join, joins))
    else:
      run_join(meta.joins()[join], props)
      joined_tabs.extend(join)
  return joined_tabs

def main(args):
  import getopt
  args = map(singlewhite, filter(lambda arg: arg, args ))
  debug("Running etl with args: %s [%s]\n" %(args, " ".join(args)))
  options, remainder = getopt.getopt(args[1:], 'c:ij:m:p:',
                                     ['gitdir=','props='])
  info("%s invoked with getopts=%s and remainder=%s" % (args[0], options, remainder))
  propfile = ""
  metafile= ""
  createtabs = []
  jointabs = []
  for opt, arg in options:
    if opt in ('-p', '--props'):
      propfile=arg
    elif opt in ('-c', '--create-tables'):
      createtabs = map(lambda t: t.strip(), arg.split(","))
    elif opt in ('-i', '--initdirs'):
      initdirs()
    elif opt in ('-m', '--metafile'):
      metafile=arg
    elif opt in ('-j', '--joins'):
      jointabs = map(lambda t: t.strip(), arg.split(","))
  info("Loading from properties file=%s and metafile=%s " %(propfile,metafile))
  props = read_props(propfile)
  meta = MsMeta.create_meta(metafile)
  if len(createtabs) > 0:
    create_tables(meta, props, createtabs)
  if (jointabs) > 0:
    run_joins(meta, props, jointabs)

if __name__ == "__main__":
  import sys
  # def usage():
  #   sys.stderr.write("Usage: %s <git src dir>\n" %sys.argv[0])
  # if len(sys.argv) != 2:
  #         usage()
  main(sys.argv)