#!/bin/python
# -*- coding: UTF-8 -*-
import threading
import time
import sys
import MySQLdb as mysql
import select
import os
import random
global yandm;
global day
global ymd
from datetime import *


#to sync the table of carriers on the other server
dbaddr="127.0.0.1"
#if len(args) <2 :
tab=mysql.connect(dbaddr,'ipv6bgp','ipv6','ipinformation',charset='utf8') 
cur_tab=tab.cursor();
cur_tab.execute("set names 'utf8'")
target="172.30.10.62"
tgt=mysql.connect(target,'ipv6bgp','ipv6','ipinformation',charset='utf8')
cur_tgt=tgt.cursor()
cur_tgt.execute("set names 'utf8'")
reload(sys)
sys.setdefaultencoding('UTF-8')
cur_tgt.execute("select max(id) from carriers")
id=cur_tgt.fetchone()[0]
expt=0	; print id 
try: # empty table
     cur_tab.execute("select * from carriers where id >%d" % (id))
except mysql.Error, e:
         expt=1;print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
finally:
		          
         print "sync "
if expt==1 :
       print 'errors';exit()
all_results=cur_tab.fetchall();i=0
for result in all_results: 
     #print result
     cur_tgt.execute("insert into carriers(id,subcarrier, addr, addr_2, code,enterprise, YMD,sample_b,average_b, sample_i, average_i,status) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % \
   (result[0],result[1], result[2],result[3],result[4],result[5],result[6],result[7],result[8],result[9],result[10],result[11])) 
     cur_tgt.execute("commit")
cur_tgt.close()
cur_tab.close()
tab.close()
tgt.close()
