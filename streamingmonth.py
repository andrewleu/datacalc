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

threadno=8
class updatedb():
   def run(self):
      dbaddr="172.24.20.185"
      tab=mysql.connect(dbaddr,'ipv6bgp','ipv6','ipinformation')
      raw=mysql.connect(dbaddr,'ipv6bgp','ipv6','data')
      cur_tab=tab.cursor();cur_tab.execute("set names 'utf8'")
      cur_raw=raw.cursor();cur_raw.execute("set names 'utf8'")
      while 1 :
            expt=0
            cur_tab.execute("select YM from streaming_month_city where id=(select max(id) from streaming_month_city)")
            YM=cur_tab.fetchone()
            cur_tab.execute("commit")
            try:
              line=cur_tab.execute("select id,name,province,city,carrier  from streaming_month_city \
              where status='w' and YM='%s' order  by rand()    limit 1 " % (YM))
            except  mysql.Error, e:
                  expt=1
                  print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            finally:
                  if line!=0 : #有内容就进行读取
                          id=cur_tab.fetchone()
                          cur_tab.execute("select name,province,city,carrier  from streaming_month_city  where id=%d for update" % id)  
                          readline=cur_tab.fetchone()
                          print readline
                  else :
                          cur_tab.execute("commit");
                          if expt==0 :
                                  break #无异常就是无数据，结束
            if expt==1 :  #异常继续
                 continue
            name=readline[0];
            province=readline[1];
            city= readline[2] ; carrier=realine[3]   
            cur_raw.execute("select count(`id`), avg(download),sum(buffer),sum(buffer_duration) from streaming where name='%s' \
            and YM='%s' and province = '%s' and city='%s' and carrier='%s' and status='b' " %  (name,YM,province,city))
            result=cur_raw.fetchone(); cur_raw.execute("commit")
            if result[0]==0   :
               b_samples=0 ; b_average=0; bpt=0;bpd=0
            else :
                 b_samples= result[0];b_average=result[1]
	         bpt=result[2];bpd=result[3]
            cur_raw.execute("select count(`id`), avg(download),sum(buffer),sum(buffer_duration) from streaming where name='%s' \
            and YM='%s' and province = '%s' and city='%s' and carrier='%s' and status='i' " %  (name,YM,province,city))
            result=cur_raw.fetchone(); cur_raw.execute("commit")
            print result
            if result[0]==0   :
                 i_samples=0 ; i_average=0; ipt=0;ipd=0
            else :
                 i_samples= result[0];i_average=result[1]			  
                 ipt=result[2];ipd=result[3]            
            cur_tab.execute("update streaming_month set b_samples='%s', b_download='%s',b_pause_duration='%s', b_pause_count='%s'\
            status='c' where id='%s'" %(b_samples,b_average,bpd,bpt,id)) #更新行
            cur_tab.execute("update streaming_month set i_samples='%s', i_download='%s',i_pause_duration='%s', i_pause_count='%s'\
            status='c' where id='%s'" %(i_samples,i_average,ipd,ipt,id)) #更新行               
            cur_tab.execute("commit")
      cur_raw.close()
      cur_tab.close()
      raw.close()
      tab.close()



dbaddr="172.24.20.185"
#if len(args) <2 :
tab=mysql.connect(dbaddr,'ipv6bgp','ipv6','ipinformation')
cur_tab=tab.cursor();
cur_tab.execute("set names 'utf8'")
reload(sys)
sys.setdefaultencoding('UTF-8')
 # cur_tab.execute("select date_add(date, interval 1 day) from streaming_daily where id=(select max(id) from streaming_daily)")
  #date=cur_tab.fetchone()
month='201507'
cur_tab.execute("select distinct data.streaming.name, data.streaming.province, data.streaming.city, data.streaming.YM,data.streaming.carrier \
from data.streaming where data.streaming.city!='' and data.streaming.YM='%s'" %(month))
results=cur_tab.fetchall()
if results=='' :
      cur_tab.close()
      tab.close()
      print "Empty DB"; exit(1)
for rslt in results: 
    try:
       cur_tab.execute("insert into streaming_month_city(name,province,city,YM,carrier) values('%s','%s','%s','%s','%s')" % ( rslt[0],rslt[1],rslt[2],rslt[3],rslt[4])) 
       expt=0;
    except mysql.Error, e:
           expt=1
           #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
    finally:
        cur_tab.execute("commit")
    if expt==1 :
       continue
cur_tab.close()
tab.close()
th=updatedb()
th.run()
   

exit()

