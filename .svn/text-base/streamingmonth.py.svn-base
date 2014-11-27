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
carrier ={
          '移动':'yd',
         	'电信':'dx',
         	'联通':'lt'
         }

threadno=8
class updatedb():
   def run(self):
      dbaddr="127.0.0.1"
      tab=mysql.connect(dbaddr,'ipv6bgp','ipv6','ipinformation')
      raw=mysql.connect(dbaddr,'ipv6bgp','ipv6','data')
      cur_tab=tab.cursor();cur_tab.execute("set names 'utf8'")
      cur_raw=raw.cursor();cur_raw.execute("set names 'utf8'")
      while 1 :
            expt=0
            try:
              line=cur_tab.execute("select `id`, `known_as`,`ent`  from streaming_month \
              where status='w' and yearmonth='%s' order  by rand()    limit 1 " % (month))
            except  mysql.Error, e:
                  expt=1
                  print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            finally:
                     if line!=0 : #有内容就进行读取
                          readline=cur_tab.fetchone()
                          print readline
                          cur_tab.execute("commit")
                     else :
                            cur_tab.execute("commit");
                            if expt==0 :
                                      break #无异常就是无数据，结束
            if expt==1 :  #异常继续
                 continue
            id=readline[0];print id
            addr=readline[1].encode()+'%';
            ent= readline[2]    ; print ent
            for telco, tpyin in carrier.items() :
	       bsample=tpyin+'_b_samples';bdownload=tpyin+'_b_download';
	       bpaused=tpyin+'_b_pause_d'; bpauset=tpyin+'_b_pause_t';
               isample=tpyin+'_i_samples';idownload=tpyin+'_i_download';
               ipaused=tpyin+'_i_pause_d'; ipauset=tpyin+'_i_pause_t';	
               cur_raw.execute("select count(`id`), avg(download),sum(buffer),sum(buffer_duration) from streaming where name='%s' \
               and YM='%s' and province like  '%s' and status='b' and carrier='%s'" %  (ent,month,addr,telco))
               result=cur_raw.fetchone(); cur_raw.execute("commit")
               if result[0]==0   :
                 b_samples=0 ; b_average=0; bpt=0;bpd=0
               else :
                 b_samples= result[0];b_average=result[1]
	         #bpt=result[2];bpd=result[3]
               cur_raw.execute("select count(`id`), avg(download) from streaming where name='%s' \
               and YM='%s' and province like '%s' and status='i' and carrier= '%s'" %  (ent,month,addr,telco))
               result=cur_raw.fetchone(); cur_raw.execute("commit")
               print result
               if result[0]==0   :
                 i_samples=0 ; i_average=0; ipt=0;ipd=0
               else :
                 i_samples= result[0];i_average=result[1]			  
                 #ipt=result[2];ipd=result[3]            
               cur_tab.execute("update streaming_month set `%s`='%s', `%s`='%s', status='c' where id='%s'" \
               %(bsample,b_samples,bdownload, b_average,id)) #更新行
               cur_tab.execute("update streaming_month set `%s`='%s', `%s`='%s', status='c' where id='%s'" \
               %(isample,i_samples,idownload, i_average,id))
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
enterprise=('优酷','Tencent')

reload(sys)
sys.setdefaultencoding('UTF-8')
 # cur_tab.execute("select date_add(date, interval 1 day) from streaming_daily where id=(select max(id) from streaming_daily)")
  #date=cur_tab.fetchone()
month='201409'
for  ent in enterprise:
    print ent;expt=0
    try: #生成空表
       cur_tab.execute("insert into streaming_month(ent,addr,code,known_as, parent,yearmonth) \
       select '%s',region.name, region.id,region.known_as,'0','%s' from region   where region.level =1" % (ent,month))
    except:
      expt=1
    finally:
       cur_tab.execute("commit")
       
    if expt==1 :
       continue
cur_tab.close()
tab.close()
th=updatedb()
th.run()
   

exit()

