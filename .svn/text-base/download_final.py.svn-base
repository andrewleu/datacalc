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
from datetime import timedelta, datetime 
class updatedb(threading.Thread):
   def run(self):
      dbaddr="127.0.0.1"
      tab=mysql.connect(dbaddr,'ipv6bgp','ipv6','ipinformation',charset='utf8')
      raw=mysql.connect(dbaddr,'ipv6bgp','ipv6','data',charset='utf8')
      cur_tab=tab.cursor();cur_tab.execute("set names 'utf8'")
      cur_raw=raw.cursor();cur_raw.execute("set names 'utf8'");
      rand=random.random();expt=0
      time.sleep(rand*10) ; #random start time
      while 1:
            expt=0
            try:
              line=(cur_tab.execute("select `id`, `subcarrier`,`addr`,addr_2,enterprise  from carriers \
              where status='w'   order by rand()    limit 1 ")) #read a line for updating
            except  mysql.Error, e:
                  expt=1
                  print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            finally:
                  if line!=0 : # there is something in the content
                          readline=cur_tab.fetchone()
                          #print readline;
                          try: 
                            cur_tab.execute("update carriers set status='c' where id=%d" % (readline[0]))
                          finally:
                            cur_tab.execute("commit")
                  else :
                          cur_tab.execute("commit");
                          if expt==0 :
                                      break #no data to process
            if expt==1 :  #exception, locked line
                continue
            id=readline[0];#print id
            subcarrier=readline[1];addr=readline[2];addr_2=readline[3]
            ent= readline[4]    ;# print ent
            try:
               cur_tab.execute("select `id`  from carriers    where id=%d for update" % (id)) #lock the line
            except  mysql.Error, e:
                  expt=1
                  print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            if expt==1 : #conflicting, go on
                continue
            #print "exception %s" %(expt)
            cur_raw.execute("select count(id), avg(avi) from download where purpose='%s' and YM=%d and day=%d and agency='%s'\
            and  addr='%s' and addr_2='%s'  and status='b'" %  (ent, int(yandm),int(day), subcarrier,addr,addr_2))
            result=cur_raw.fetchone(); cur_raw.execute("commit")
            
            #print result
            if result[0]==0     :
                samples=0; average=0;
            else :
               samples= result[0];average=result[1]
            cur_tab.execute("update carriers set sample_b='%s', average_b='%s' where id='%s'" %(samples, average, id)) #updating busy field
            cur_raw.execute("select count(id), avg(avi) from download where purpose='%s' and YM=%d and day=%d  and agency='%s'\
            and  addr='%s' and addr_2='%s'  and status='i'" %  (ent, int(yandm) ,int(day), subcarrier,addr,addr_2))
            result=cur_raw.fetchone(); cur_raw.execute("commit")
            if result[0]==0     :
                samples=0; average=0;
            else :
               samples= result[0];average=result[1]
            cur_tab.execute("update carriers set sample_i='%s', average_i='%s' where id='%s'" %(samples, average, id)) #updating idle field         
            cur_tab.execute("commit")
      cur_raw.close()
      cur_tab.close()
      raw.close()
      tab.close()
global mutex
mutex=threading.Lock()


dbaddr="127.0.0.1"
#if len(args) <2 :
tab=mysql.connect(dbaddr,'ipv6bgp','ipv6','ipinformation',charset='utf8')
cur_tab=tab.cursor();
cur_tab.execute("set names 'utf8'")

reload(sys)
sys.setdefaultencoding('UTF-8')
threadno=6
#telco1=u"\u7535\u4fe1"; telco2=u"\u8054\u901a";telco3=u"\u79fb\u52a8";
line=(cur_tab.execute("select YMD from carriers where status='w' limit 1"))
if line==0:
  cur_tab.execute("select YMD from carriers where id=(select max(id) from carriers)")
  ymd=cur_tab.fetchone()[0]
  delta=timedelta(days=1)
  ymd= ymd+delta
  ymd=ymd.isoformat()
  ymd=ymd.strip().replace('-','')
  yandm=ymd[0:6];day=ymd[6:8]   
  expt=0 
  try: # empty table
     cur_tab.execute("insert into carriers(subcarrier, addr, addr_2, enterprise, YMD) \
     select distinct data.`download`.`agency` AS `agency`,data.`download`.`addr` AS `addr`,\
        data.`download`.`addr_2` AS `addr_2`,data.`download`.`purpose` AS `purpose`,'%s' \
        from data.`download` where (data.`download`.`YM` = '%s' and data.`download`.day='%s' and \
        (data.download.status='i' or data.download.status='b'))" % (ymd,int(yandm),int(day)) )
  except mysql.Error, e:
      expt=1;print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
  finally:
       cur_tab.execute("commit")
       print "go on"
  if expt==1 :
     exit()
 #insert area code                                                                                                     
  while 1 :                                                                                                             
    ex=0                                                                                                               
    line=(cur_tab.execute("select id, addr, addr_2 from carriers where code is NULL order by rand() limit 1"))       
    if line==0 :                                                                                                       
           break;                                                                                                          
    readline=cur_tab.fetchone();#print readline                                                                        
    try:                                                                                                               
      cur_tab.execute("update carriers set code=0 where id='%s'" % (readline[0]));                                  
    except:                                                                                                            
          ex=1                                                                                                            
    finally:                                                                                                           
          cur_tab.execute("commit");                                                                                      
    if ex==1 :                                                                                                         
           continue                                                                                                  
    if readline[2]=="-" :                                                                                              
        line=(cur_tab.execute( "select id from region where name like '%s' and level=1" % (readline[1]+'%')))            
        if line!=0 :                                                                                                     
           cur_tab.execute("update carriers set code ='%s' where id = '%s' " % (cur_tab.fetchone()[0],readline[0]))     
    else :                                                                                                             
        if len(readline[2])>=5 :                                                                                         
               addr=readline[2][0:5]                                                                                         
               line=(cur_tab.execute("select id from region where name like '%s' and level=2" % (addr+'%')))                 
               if line!=0 :                                                                                                  
                 cur_tab.execute("update carriers set code ='%s' where id='%s' " % (cur_tab.fetchone()[0],readline[0]))    
        else :                                                                                                           
               line=(cur_tab.execute("select id from region where name like '%s' and level=2" % (readline[2]+'%')))          
               if line!=0 :                                                                                                  
                 cur_tab.execute("update carriers set code ='%s' where id='%s'" % (cur_tab.fetchone()[0],readline[0]))     
    cur_tab.execute("commit")                                                                                          
else:
  ymd=cur_tab.fetchone()[0]
  delta=timedelta(days=1)
  ymd=ymd.isoformat()
  ymd=ymd.strip().replace('-','')
  yandm=ymd[0:6];day=ymd[6:8]  
cur_tab.close()
tab.close();
start=datetime.now()  
for thrd in range(threadno) :
    try:
      th=updatedb()
      th.start()
    except:
        sleep(3)
        print "Exception occurs"
        completed=0
    finally:
      time.sleep(2); thrd=0
for thrd in range(threadno) : #strat multi-thread
   th.join()
end=datetime.now()
print start
print end 
exit()
