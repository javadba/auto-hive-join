#!/usr/bin/env python
# DB.py: Provide connectivity and basic query/update methods to an rdbms
import MySQLdb
import sys
from datetime import datetime
from logger import *

# Provides basic/simple relational db connectivity and sql operations
class DB:
  def __init__(self, dbprops):
    self.dbprops = dbprops
    self.conn = self.get_connection(self.dbprops)
    debug("self.conn is %s" %self.conn)
  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    if not self.conn is None:
      self.close()

    # if exc_val is not None:
    #   tracerr("DB exited improperly (exc_val=%s)" %exc_val, True)

  def get_cmdline_mysql(self):
    # mysql_runner="mysql -uamroot -pad8mobius2 -hclassint.ckqg0idoxglm.us-east-1.rds.amazonaws.com classint"
    return "mysql -u%s -p%s -h%s %s" % (self.property("db.user"),
                                        self.property("db.password"),
                                        self.property("db.host"),
                                        self.property("db.dbname"))
  def property(self, name):
    return self.dbprops[name]

  def properties(self, *names):
    return [self.property(name) for name in names]

  def get_connection(self, dbprops):
    tracerr("Connecting to %s ... " %dbprops['db.url'])
    if not hasattr(self, 'conn'):
      # Prep the connection
      #connect_method = java.sql.DriverManager.java_send(:get_connection, [java.lang.String, java.lang.String, java.lang.String])
      #conn = connect_method.call(dbprops['db.url'], dbprops['db.user'], dbprops['db.password'])
      self.conn = MySQLdb.connect(dbprops['db.host'], dbprops['db.user'], dbprops['db.password'], dbprops['db.dbname'])
      debug("Connection to %s successful" %(dbprops))
    return self.conn

  # Define the query
  selectquery = "SELECT * from model order by name"

  def query(self,sql):
    outrows = []
    cursor = None
    try:
      cursor = self.conn.cursor()
      cursor.execute(sql)
      rows = cursor.fetchall()
      cols = [t for t in cursor.description]
      for row in rows:
        rmap = {}
        for col, val in zip(cols,row):
          rmap[col] = val
        outrows.append(rmap)
      return outrows
    except Exception, e:
      tracerr("Error executing sql [%s]" %(sql),True)
      raise
    finally:
      if cursor is not None:
        cursor.close()

  def close(self):
    if self.conn is not None:
      self.conn.close()

  def testdb(self):
    db = DB()
    out = db.query("select * from model")
    info("results are %s" %(out))

def get_cmdline_mysql(self):
  # mysql_runner="mysql -uamroot -pad8mobius2 -hclassint.ckqg0idoxglm.us-east-1.rds.amazonaws.com classint"
  with DB(self.dbprops) as db:
    if not db:
      raise Exception("Db is None inside with")
    return db.get_cmdline_mysql()

# raise Exception("Unable to connect to db using props %s" % dbprops)
# Read the classification models metadata from mysql

def read_models_meta(self):
  modelsmap = {}
  with DB(self.dbprops) as db:
    modelrows = db.query("select * from model order by name, version desc")
    oldname = None
    currow = {}
    for rowmap in modelrows:
      name = rowmap['name']
      #$stderr.puts "name is [#{rowmap['name']}] rowmap is #{rowmap}"
      if oldname != None and name != oldname:
        modelsmap[name] = rowmap
      oldname = name
      currow  = rowmap
    if currow != None:
      modelsmap[currow['name']] =  currow
  return modelsmap
