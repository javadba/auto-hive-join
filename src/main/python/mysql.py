#!/usr/bin/python

import sys 
import MySQLdb
import traceback

# Open database connection
db = MySQLdb.connect("localhost","root","","hive" )

# prepare a cursor object using cursor() method
cursor = db.cursor()

# Prepare SQL query to INSERT a record into the database.
sql = "SELECT * FROM app_meta where status='MATCH' limit 3"
try:
   # Execute the SQL command
   cursor.execute(sql)
   # Fetch all the rows in a list of lists.
   results = cursor.fetchall()
   for row in results:
      id, urlid, crawlid, status = row[0:4]
      # Now print fetched result
      print "id=%s,urlid=%s,crawlid=%d,status=%s" % (id,urlid, crawlid,status)
except:
   print "Error: unable to fetch data  %s" % sys.exc_info()[0]
   print "Error: unable to fetch data  %s" % '\n'.join(traceback.format_exc().splitlines())

# disconnect from server
db.close()

