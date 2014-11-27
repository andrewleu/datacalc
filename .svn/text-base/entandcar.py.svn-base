#!/bin/python
# -*- coding: UTF-8 -*-
import threading
import time
import sys
import MySQLdb as mysql
import select
import os
import random
global date
global carreir
carrier ={
          '移动':'yidong',
         	'电信':'dianxin',
         	'联通':'liantong'
         }
date=''
threadno= 8
class updatedb(threading.Thread):
   def run(self):
      dbaddr="127.0.0.1"
      tab=mysql.connect(dbaddr,'ipv6bgp','ipv6','ipinformation')
      raw=mysql.connect(dbaddr,'ipv6bgp','ipv6','data')
      cur_tab=tab.cursor();cur_tab.execute("set names 'utf8'")
      cur_raw=raw.cursor();cur_raw.execute("set names 'utf8'")
      yandm=date.replace('-','')
      day=yandm[6:8]
      yandm=yandm[0:6]
      day=day.lstrip('0')
      firstline=1
      while 1 :
            rand=random.random();expt=0
            time.sleep(rand*10) ; #随机等待减少表锁冲突
            try:
              line=(cur_tab.execute("select `id`, `known_as`,`ent`,`addr`  from entandcarrier \
              where status='w' and date='%s'order  by rand()    limit 1 " % (date)))
            except  mysql.Error, e:
                  expt=1
                  print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            finally:
                     if line!=0 : #有内容就进行读取
                          readline=cur_tab.fetchone()
                          #print readline
                          cur_tab.execute("commit")
                     else :
                            cur_tab.execute("commit");
                            if expt==0 :
                                      break #无异常就是无数据，结束
            if expt==1 :  #异常继续
                 continue
            id=readline[0];
            known_as=readline[1].encode()+'%';
            ent= readline[2]    ;  addr=readline[3]
            if firstline==1 : #only print once
                 print readline
                 print "id %s, addr %s, enterprise %s" %( id, addr, ent)
                 print len(addr)
                 firstline=0
            try:
               cur_tab.execute("select `id`, `known_as`,`ent`  from entandcarrier \
               where id='%s' for update" % (id)) #加上行锁
            except  mysql.Error, e:
                  expt=1
                  print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            if expt==1 : #冲突异常，继续
                 cur_tab.execute("commit"); continue
            cur_tab.execute("update entandcarrier set status='p' where id='%s'" % (id))
            cur_tab.execute("commit") # the line status updated
            cur_tab.execute("select `id`, `known_as`,`ent`  from entandcarrier \
            where id='%s' for update" % (id)) #加上行锁 again
            for telco, tpyin in carrier.items() :
               if len(addr)>15 :
                 cur_raw.execute("select count(id), avg(avi) from download where purpose='%s' and YM=%s \
                 and day=%s and agency='%s' and  status='b'and addr_2 like '%s'" %  (ent, yandm, day,telco,known_as))
               else :
                 cur_raw.execute("select count(id), avg(avi) from download where purpose='%s' and YM=%s \
                 and day=%s and agency='%s' and  status='b'and addr_2 = '%s'" %  (ent, yandm, day,telco,addr))   
               result=cur_raw.fetchone(); cur_raw.execute("commit")
               if result[0]==0   :
                 bsamples=0; baverage=0;
               else :
                 bsamples= result[0];baverage=result[1]
               telco_b=tpyin+'_busy';telco_b_s=tpyin+'_busy_sample'
               if len(addr)>15 :
                 cur_raw.execute("select count(id), avg(avi) from download where purpose='%s' and YM=%s \
                 and day=%s and agency='%s' and  status='i' and addr_2 like '%s'" %  (ent, yandm, day,telco,known_as))
               else :
                 cur_raw.execute("select count(id), avg(avi) from download where purpose='%s' and YM=%s\
                 and day=%s and agency='%s' and  status='i' and addr_2 = '%s'" %  (ent, yandm, day,telco,addr))
               result=cur_raw.fetchone(); cur_raw.execute("commit")
               if result[0]==0     :
                 isamples=0; iaverage=0;
               else :
                 isamples= result[0];iaverage=result[1]
               telco_i=tpyin+'_idle';telco_i_s=tpyin+'_idle_sample'               
               cur_tab.execute("update entandcarrier set `%s`='%s', `%s`='%s',  `%s`='%s', `%s`='%s' where id='%s'" \
               %(telco_b, baverage,telco_b_s,bsamples,telco_i,iaverage,telco_i_s,isamples, id)) #更新行
	    cur_tab.execute("update entandcarrier set status='c' where id='%s'" % (id))
            cur_tab.execute("commit")
      cur_raw.close()
      cur_tab.close()
      raw.close()
      tab.close()



dbaddr="127.0.0.1"
#if len(args) <2 :
tab=mysql.connect(dbaddr,'ipv6bgp','ipv6','ipinformation')
cur_tab=tab.cursor();
cur_tab.execute("set names 'utf8'")
enterprise=('qihu','chinacache','netcenter')

reload(sys)
sys.setdefaultencoding('UTF-8')
line=cur_tab.execute("select distinct date from entandcarrier where  status='w'")
if line==0 :
  cur_tab.execute("select date_add(date, interval 1 day) from entandcarrier where id=(select max(id) from entandcarrier)")
  stime=cur_tab.fetchone()
  if stime :
    date=stime[len(stime)-1]
    date=date.strftime('%Y-%m-%d') #时间格式转换
  #date='2014-04-01'
  for  ent in enterprise:
    print ent;expt=0
    try: #生成空表
       cur_tab.execute("insert into entandcarrier(ent,addr,code, parent,degree,known_as,date) \
       select '%s',region.name, region.id,region.pname,'2', region.known_as,'%s' from region   where region.level =2" % (ent,date))
    except:
      expt=1
    finally:
       cur_tab.execute("commit")
       
    if expt==1 :
       continue
else :
   stime=cur_tab.fetchone()
   date=stime[len(stime)-1]
   date=date.strftime('%Y-%m-%d') #时间格式转换
print date
cur_tab.close()
tab.close()
for thrd in range(threadno) :
    try:
      th=updatedb()
      th.start()
    except:
        time.sleep(3)
        print "Exception occurs"
        completed=0
    finally:
      time.sleep(1)
for thrd in range(threadno) : #strat multi-thread
   th.join()

exit()

