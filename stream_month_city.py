#!/bin/python
# -*- coding: UTF-8 -*-
import threading
import time
import sys
import MySQLdb as mysql
import select
import os
import random
global month
global ent
threadno=5
class updatedb(threading.Thread):
   __tab='';
   __raw='';__cur_tab='';__cur_raw=''
   def run(self):
      __tab=mysql.connect(dbaddr,'ipv6bgp','ipv6','ipinformation',charset="utf8")
      __raw=mysql.connect(dbaddr,'ipv6bgp','ipv6','data',charset='utf8')
      __cur_tab=__tab.cursor();
      __cur_tab.execute("set names 'utf8'")
      __cur_raw=__raw.cursor();__cur_raw.execute("set names 'utf8'")
      #cur_tab=tab.cursor(); 
      #cur_tab.execute("set names 'utf8'")
      #cur_raw=raw.cursor();cur_raw.execute("set names 'utf8'");print "connDB"
      _line=__cur_tab.execute("select YM from streaming_month_city where id=(select max(id) \
      from streaming_month_city where status='w')");print _line
      if _line: 
          YM=__cur_tab.fetchone()[0]
      __cur_tab.execute("commit")
      i=0
      while 1:
            expt=0 ; i=i+1  
            try:
              _line=__cur_tab.execute("select id from streaming_month_city  where status='w' and YM='%s' order  by rand()    limit 1 " % (YM))
            except  mysql.Error, e:
                  expt=1
                  print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            try:      
               if _line!=0 : #有内容就进行读取
                          id=__cur_tab.fetchone()[0];__cur_tab.execute("commit")
                          __cur_tab.execute("select name,province,city,carrier  from streaming_month_city \
                          where id='%s' for update" %(id))  
                          readline=__cur_tab.fetchone()
                          #print i; print readline
               else :
                          __cur_tab.execute("commit");
                          if expt==0 :
                                  break #无异常就是无数据，结束
            except mysql.Error, e:
                  expt=1
                  print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            if expt==1 :  #异常继续
                 continue
            name=readline[0];                                            
            province=readline[1]                                            
            city= readline[2] ; carrier=readline[3]                        
            __cur_raw.execute("select count(`id`), avg(download),sum(buffer),sum(buffer_duration) from streaming  where name='%s' \
            and YM='%s' and province ='%s' and city='%s' and carrier='%s' and status='b' " %  (name,YM,province,city,carrier))
            result=__cur_raw.fetchone(); __cur_raw.execute("commit")
            if result[0]==0   :
               b_samples=0 ; b_average=0; bpt=0;bpd=0
            else :
                 b_samples= result[0];b_average=result[1];bpt=result[2];bpd=result[3]
            __cur_raw.execute("select count(`id`), avg(download),sum(buffer),sum(buffer_duration) from streaming where name='%s' \
            and YM='%s' and province = '%s' and city='%s' and carrier='%s' and status='i' " %  (name,YM,province,city,carrier))
            result=__cur_raw.fetchone(); __cur_raw.execute("commit")
            #print result;print id ;print YM
            if result[0]==0   :
                 i_samples=0 ; i_average=0; ipt=0;ipd=0
            else :
                 i_samples= result[0];i_average=result[1]			  
                 ipt=result[2];ipd=result[3]            
            __cur_tab.execute("update streaming_month_city set b_samples='%s', b_download='%s',b_pause_duration='%s', b_pause_count='%s',\
            status='c' where id='%s'" % (b_samples,b_average,bpd,bpt,id)) #更新行
            __cur_tab.execute("update streaming_month_city set i_samples='%s', i_download='%s',i_pause_duration='%s', i_pause_count='%s',\
            status='c' where id='%s'" %(i_samples,i_average,ipd,ipt,id)) #更新行               
            __cur_tab.execute("commit")
      __cur_raw.close()
      __cur_tab.close()
      __raw.close()
      __tab.close()


dbaddr="172.24.20.185"
tab=mysql.connect(dbaddr,'ipv6bgp','ipv6','ipinformation',charset="utf8")
raw=mysql.connect(dbaddr,'ipv6bgp','ipv6','data',charset='utf8')
cur_tab=tab.cursor();
cur_tab.execute("set names 'utf8'")
cur_raw=raw.cursor();cur_raw.execute("set names 'utf8'")
reload(sys)
sys.setdefaultencoding('utf8')
 # cur_tab.execute("select date_add(date, interval 1 day) from streaming_daily where id=(select max(id) from streaming_daily)")
  #date=cur_tab.fetchone()
while 1 :
  line=cur_tab.execute("select distinct max(YM) from streaming_month_city")
  month=cur_tab.fetchone()[0] #select the last year and month value in the table
  line=cur_tab.execute("select data.streaming.YM , data.streaming.id from data.streaming where id>(select max(data.streaming.id) \
  from data.streaming where data.streaming.YM='%s') limit 1" % (month))
  # Select the last year and month id+1 from the rawdata, the month should be increase by one
  ex=0; YM='201507' 
  if line : #any value?
    YM=cur_tab.fetchone()[0]; 
    cur_tab.execute("select data.streaming.YM , data.streaming.id from data.streaming \
    where id=(select max(data.streaming.id) from data.streaming)")
    lastmonth=cur_tab.fetchone()[0]
    print "last YM in month table: '%s', last rawdata YM: '%s'" % (YM,lastmonth)
    if YM==lastmonth: 
    #the last YM tag is same as the selected YM tag, the month is still not over
      ex=1
    else:
      month=YM
      cur_tab.execute("select now()")
      nowaday=cur_tab.fetchone()[0].day;
      if nowaday<=3 :
        ex=1; #have to start after the third day of the month
  else:
     ex=1
  cur_tab.execute("commit")
  if ex:
     print "waiting";break
  expt=0
  line=cur_tab.execute("select id from streaming_month_city where status='w' limit 1");
  cur_tab.execute("commit")
  if line==0 :
     #cur_tab.execute("select YM from streaming_month_city where id=(select max(id) from streaming_month_city)")
     #YMtab=cur_tab.fetchone()[0];cur_tab.execute("commit")
     #if YMtab==month :
     #   print "done"; break     
     line=cur_tab.execute("select distinct data.streaming.name, data.streaming.province, data.streaming.city, data.streaming.YM,data.streaming.carrier \
     from data.streaming where data.streaming.city!='' and data.streaming.YM='%s' and (status='b' or status='i') " %(month))
     results=cur_tab.fetchall()
     if line==0 :
       print "Empty DB"; break
     for rslt in results: 
      try:
        cur_tab.execute("insert into streaming_month_city(name,province,city,YM,carrier) \
        values('%s','%s','%s','%s','%s')" % (rslt[0],rslt[1],rslt[2],rslt[3],rslt[4])) 
      except mysql.Error, e:
        expt=1
        print e
      finally:
        cur_tab.execute("commit")
     # if expt==1 :
      #  continue
  for thrd in range(threadno) :
    print "Threading"
    th=updatedb()
    th.start();time.sleep(1)    
  for thrd in range(threadno) : #strat multi-thread
    th.join(); print thrd;time.sleep(1)
cities=(u'天津市',u'上海市',u'重庆市',u'北京市')
try :
  error=0
  ret=cur_tab.execute("commit")
except Exception,e :
  print e; error=1
if error :
  tab=mysql.connect(dbaddr,'ipv6bgp','ipv6','ipinformation',charset="utf8")
  raw=mysql.connect(dbaddr,'ipv6bgp','ipv6','data',charset='utf8')
  cur_tab=tab.cursor();
  cur_tab.execute("set names 'utf8'")
  cur_raw=raw.cursor();cur_raw.execute("set names 'utf8'") 
for city in cities :
     ret=cur_tab.execute("select id from region where name='%s' " %(city))
     if ret!=0 :
        res=cur_tab.fetchone()[0];cur_tab.execute("commit")
        cur_tab.execute("update streaming_month_city set code='%d'  where province='%s' and code=0" % (res,city))
        cur_tab.execute("commit")
while 1 :  
  line=cur_tab.execute("select id, substring(province,1,2),substring(city,1,2) from streaming_month_city where code=0 order by rand() limit 1")
  try :
    if line!=0 :
      results=cur_tab.fetchone();cur_tab.execute("commit")
      cur_tab.execute("select id, province,city from streaming_month_city where id='%s' for update " % (results[0]))
      retn=cur_tab.execute("select id from region where (pname like '%s' and name like '%s') limit 1" % (results[1]+'%',results[2]+'%'))
      if retn!=0 :
          rslt=cur_tab.fetchone()[0];#print rslt
          cur_tab.execute("update streaming_month_city set code='%s' where id='%s'" % (rslt,results[0]))
      else :
          retn=cur_tab.execute("select id from region where (pname like '%s' ) limit 1" % (results[1]+'%'))
          if retn !=0 : 
              cur_tab.execute("update streaming_month_city set code='%s' where id='%s'" % (cur_tab.fetchone()[0],results[0]))
          else :
             cur_tab.execute("update streaming_month_city set code=1 where id='%s'" % (results[0]))
    else :
      cur_tab.execute("commit")
      break
  except mysql.Error, e:
     print e;
  finally :
     cur_tab.execute("commit")
cur_raw.close()
cur_tab.close()
raw.close()
tab.close()

exit()


