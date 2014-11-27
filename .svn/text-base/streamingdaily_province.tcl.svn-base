package require mysqltcl
proc daily_metro_proc {region  telco ent} {
    global raw_data; global mysql_handler
    global YM; global day
    #puts $region
    set all [mysqlsel $raw_data "select count(id),sum(watching), avg(download),sum(buffer),sum(buffer_duration) from streaming
         where YM='$YM' and day= $day and carrier = '$telco' and  province like '$region%' and name='$ent' " -list]
    set all [string map {"{}" "0"} $all]
    set busy [mysqlsel $raw_data "select count(id),sum(watching), avg(download),sum(buffer),sum(buffer_duration) from streaming
         where YM='$YM' and day= $day and carrier = '$telco' and  province like '$region%' and status='b' and name='$ent' " -list]
    set busy [string map {"{}" "0"} $busy]
    set idle [mysqlsel $raw_data "select count(id),sum(watching), avg(download),sum(buffer),sum(buffer_duration) from streaming
         where YM='$YM' and day= $day and carrier = '$telco' and  province like '$region%' and status='i' and name='$ent'" -list]
          set idle [string map {"{}" "0"} $idle]
    set result [concat $all $busy $idle]
#    puts $result 
    return $result
  }

  
set port {3306}
set host {127.0.0.1}
set user {ipv6bgp}
set password {ipv6}
set dbname {ipinformation} ;# calculated data
set data {data} ;#raw data
set mysql_handler [mysqlconnect -host $host -port $port -user $user -password $password]
set raw_data  [mysqlconnect -host $host -port $port -user $user -password $password]
set rowid 1
#起启数据库的行
mysqlexec $mysql_handler "set names 'utf8'"
mysqlexec $raw_data "set names 'utf8'"
mysqluse $mysql_handler $dbname
mysqluse $raw_data $data
set finish 0


set date [mysqlsel $mysql_handler "select distinct date from streaming_daily  
    where status='w' and parent=0 " -list]
puts $date
set datecode [string map {"-" ""} $date]
set YM [string range $datecode 0 5] ;#year and month
if {[string range $datecode 6 6] == "0" } {
   set day [string range $datecode 7 7]
} else {
  set day [string range $datecode 6 7] ;# date of a month
}

foreach code {"110000" "120000" "310000" "500000"} {
        # beijing, shanghai, tianjin, chongqing
  foreach ent {"Tencent" "优酷"} { 
     mysqlexec $mysql_handler "begin"
     set query [mysqlsel  $mysql_handler "select `id`, `addr`,`code`, `known_as`,`date` from streaming_daily
      where code='$code'  and ent='$ent' and date='$date' for update  " -list]  
    
    if { $query ==""} {
          mysqlexec $mysql_handler "commit"
          break
    }
   
   set result_yd [daily_metro_proc [lindex $query 0 3] "移动" $ent ]
   set result_lt [daily_metro_proc [lindex $query 0 3] "联通" $ent ]
   set result_dx [daily_metro_proc [lindex $query 0 3] "电信" $ent ] 
   mysqlexec $mysql_handler "update streaming_daily set 
          yd_b_samples=[lindex $result_yd 1 0],      yd_b_duration =[lindex $result_yd 1 1], yd_b_download=[lindex $result_yd 1 2],
               yd_b_pause_t=[lindex $result_yd 1 3], yd_b_pause_d=[lindex $result_yd 1 4],
               yd_i_samples=[lindex $result_yd 2 0],      yd_i_duration =[lindex $result_yd 2 1], yd_i_download=[lindex $result_yd 2 2],
               yd_i_pause_t=[lindex $result_yd 2 3], yd_i_pause_d=[lindex $result_yd 2 4],
          lt_b_samples=[lindex $result_lt 1 0],      lt_b_duration =[lindex $result_lt 1 1], lt_b_download=[lindex $result_lt 1 2],
               lt_b_pause_t=[lindex $result_lt 1 3], lt_b_pause_d=[lindex $result_lt 1 4],
               lt_i_samples=[lindex $result_lt 2 0],      lt_i_duration =[lindex $result_lt 2 1], lt_i_download=[lindex $result_lt 2 2],
               lt_i_pause_t=[lindex $result_lt 2 3], lt_i_pause_d=[lindex $result_lt 2 4],
          dx_b_samples=[lindex $result_dx 1 0],      dx_b_duration =[lindex $result_dx 1 1], dx_b_download=[lindex $result_dx 1 2],
               dx_b_pause_t=[lindex $result_dx 1 3], dx_b_pause_d=[lindex $result_dx 1 4],
               dx_i_samples=[lindex $result_dx 2 0],      dx_i_duration =[lindex $result_dx 2 1], dx_i_download=[lindex $result_dx 2 2],
               dx_i_pause_t=[lindex $result_dx 2 3], dx_i_pause_d=[lindex $result_dx 2 4], status='c'
               where id=[lindex $query 0 0]"
     mysqlexec $mysql_handler "commit"
  } 

}

#set day [expr {$day+0}]
while {$finish!=1 } {
     set query [mysqlsel  $mysql_handler "select `id`, `addr`,`code`, `known_as`,`date`,ent from streaming_daily 
	     where parent='0'  and status='w'  and code !=810000 and code != 820000 and code !=710000 
             order by rand() limit 1 " -list]   
	# removed 'status=w' statement ,for async processing every thread will calculate the average value
    if { $query ==""} {
        mysqlexec $mysql_handler "commit"
        break
     }
     set dl_yd [mysqlsel $mysql_handler "select    sum(yd_b_samples), sum(yd_b_download*yd_b_samples),
        sum(yd_i_samples), sum(yd_i_download*yd_i_samples) from streaming_daily 
        where parent='[lindex $query 0 2]' and date='[lindex $query 0 4]' and ent='[lindex $query 0 5]'" -list]

     if {[lindex $dl_yd 0 1]==0 } {
     	    set dl_b_avg 0
     	} else {
       	set dl_b_avg [expr {[lindex $dl_yd 0 1]/[lindex $dl_yd 0 0]}]
     	}
     if {[lindex $dl_yd 0 3]==0 } {
     	  set dl_i_avg 0
     	} else {
     	set dl_i_avg [expr {[lindex $dl_yd 0 3]/[lindex $dl_yd 0 2 ]}]
     	}
      set result [mysqlsel $mysql_handler "select 
        sum(yd_b_samples), sum(yd_b_duration),'$dl_b_avg',sum(yd_b_pause_d), sum(yd_b_pause_t), 
	    sum(yd_i_samples), sum(yd_i_duration),'$dl_i_avg',sum(yd_i_pause_d), sum(yd_i_pause_t)
	    from streaming_daily where parent=[lindex $query 0 2] and date='[lindex $query 0 4]'  and ent='[lindex $query 0 5]'" -list]
      mysqlexec $mysql_handler "update streaming_daily set 
          yd_b_samples=[lindex $result 0 0], 	 yd_b_duration =[lindex $result 0 1], yd_b_download= [lindex $result 0 2],
	      yd_b_pause_d=[lindex $result 0 3],  yd_b_pause_t=[lindex $result 0 4], 
          yd_i_samples=[lindex $result 0 5], 	 yd_i_duration =[lindex $result 0 6], yd_i_download= [lindex $result 0 7],
	      yd_i_pause_d=[lindex $result 0 8],yd_i_pause_t=[lindex $result 0 9]
	    where id=[lindex $query 0 0]" 
     set dl_lt [mysqlsel $mysql_handler "select  
      sum(lt_b_samples), sum(lt_b_download*lt_b_samples),
      sum(lt_i_samples), sum(lt_i_download*lt_i_samples) from streaming_daily 
     where parent='[lindex $query 0 2]' and date='[lindex $query 0 4]'  and ent='[lindex $query 0 5]'" -list]
     if {[lindex $dl_lt 0 1]==0 } {
     	  set dl_b_avg 0
     	} else {
     		set dl_b_avg [expr {[lindex $dl_lt 0 1]/[lindex $dl_lt 0 0]}]
     	}
     if {[lindex $dl_lt 0 3]==0 } {
     	  set dl_i_avg 0
     	} else {
     	set dl_i_avg [expr {[lindex $dl_lt 0 3]/[lindex $dl_lt 0 2 ]}]
     	}
      set result [mysqlsel $mysql_handler "select 
        sum(lt_b_samples), sum(lt_b_duration),'$dl_b_avg',sum(lt_b_pause_d), sum(lt_b_pause_t), 
	    sum(lt_i_samples), sum(lt_i_duration),'$dl_i_avg',sum(lt_i_pause_d), sum(lt_i_pause_t)
	    from streaming_daily where parent=[lindex $query 0 2] and date='[lindex $query 0 4]'  and ent='[lindex $query 0 5]'" -list]
      mysqlexec $mysql_handler "update streaming_daily set 
       lt_b_samples=[lindex $result 0 0], 	 lt_b_duration =[lindex $result 0 1], lt_b_download= [lindex $result 0 2],
	   lt_b_pause_d=[lindex $result 0 3],  lt_b_pause_t=[lindex $result 0 4], 
       lt_i_samples=[lindex $result 0 5], 	 lt_i_duration =[lindex $result 0 6], lt_i_download= [lindex $result 0 7],
	   lt_i_pause_d=[lindex $result 0 8],lt_i_pause_t=[lindex $result 0 9]
	   where id=[lindex $query 0 0]" 
     set dl_dx [mysqlsel $mysql_handler "select  
      sum(dx_b_samples), sum(dx_b_download*dx_b_samples),
      sum(dx_i_samples), sum(dx_i_download*dx_i_samples) from streaming_daily 
     where parent='[lindex $query 0 2]' and date='[lindex $query 0 4]'  and ent='[lindex $query 0 5]'" -list]
   	 if {[lindex $dl_dx 0 1]==0 } {
     	  set dl_b_avg 0
     	} else {
     		set dl_b_avg [expr {[lindex $dl_dx 0 1]/[lindex $dl_dx 0 0]}]
     	}
     if {[lindex $dl_dx 0 3]==0 } {
     	  set dl_i_avg 0
     	} else {
     	set dl_i_avg [expr {[lindex $dl_dx 0 3]/[lindex $dl_dx 0 2 ]}]
     	}
     set result [mysqlsel $mysql_handler "select 
        sum(dx_b_samples), sum(dx_b_duration),'$dl_b_avg',sum(dx_b_pause_d), sum(dx_b_pause_t), 
	    sum(dx_i_samples), sum(dx_i_duration),'$dl_i_avg',sum(dx_i_pause_d), sum(dx_i_pause_t)
	    from streaming_daily where parent=[lindex $query 0 2] and date='[lindex $query 0 4]'  and ent='[lindex $query 0 5]'" -list]
     mysqlexec $mysql_handler "update streaming_daily set 
        dx_b_samples=[lindex $result 0 0], 	 dx_b_duration =[lindex $result 0 1], dx_b_download= [lindex $result 0 2],
	    dx_b_pause_d=[lindex $result 0 3],  dx_b_pause_t=[lindex $result 0 4], 
        dx_i_samples=[lindex $result 0 5], 	 dx_i_duration =[lindex $result 0 6], dx_i_download= [lindex $result 0 7],
	    dx_i_pause_d=[lindex $result 0 8], dx_i_pause_t=[lindex $result 0 9],status='c'
	    where id=[lindex $query 0 0]" 	    	    
     mysqlexec $mysql_handler "commit"
 	     
}
set r [expr rand()*20000]
after  [expr int($r)]
mysqlexec $mysql_handler "update streaming_daily set status='c' 
  where ( code=710000 or code=810000 or code=820000) and status='w'" 
mysqlclose $mysql_handler


if 0 {
CREATE TABLE `streaming_daily` (
  `id` bigint(8) NOT NULL AUTO_INCREMENT,
  `ent` varchar(10) NOT NULL DEFAULT '',
  `addr` varchar(20) NOT NULL DEFAULT '',
  `code` int(6) DEFAULT '0',
  `parent` int(6) DEFAULT '0',
  `date` date NOT NULL DEFAULT '0000-00-00',
  `yd_samples` int(7) DEFAULT '0' COMMENT '移动样本数',
  `yd_duration` bigint(10) DEFAULT '0' COMMENT '移动收看总时长',
  `yd_download` float(9,5) DEFAULT '0.00000' COMMENT '移动下载',
  `yd_pause_d` bigint(10) DEFAULT '0' COMMENT '移动停顿总时长',
  `yd_pause_t` bigint(7) DEFAULT '0' COMMENT '移动停顿次数',
  `yd_b_samples` int(7) DEFAULT '0' COMMENT '移动忙时样本数',
  `yd_b_duration` bigint(10) DEFAULT '0' COMMENT '移动忙时收看总时长',
  `yd_b_download` float(9,5) DEFAULT '0.00000' COMMENT '移动忙时下载',
  `yd_b_pause_d` bigint(10) DEFAULT '0' COMMENT '移动忙时停顿总时长',
  `yd_b_pause_t` bigint(7) DEFAULT '0' COMMENT '移动忙时停顿次数',
  `yd_i_samples` int(7) DEFAULT '0' COMMENT '移动闲时样本数',
  `yd_i_duration` bigint(10) DEFAULT '0' COMMENT '移动闲时收看总时长',
  `yd_i_download` float(9,5) DEFAULT '0.00000' COMMENT '移动闲时下',
  `yd_i_pause_d` bigint(10) DEFAULT '0' COMMENT '移动闲时停顿总时长',
  `yd_i_pause_t` bigint(7) DEFAULT '0' COMMENT '移动闲时停顿次数',
  `lt_samples` int(7) DEFAULT '0' COMMENT '联通样本数',
  `lt_duration` bigint(10) DEFAULT '0' COMMENT '联通收看总时长',
  `lt_download` float(9,5) DEFAULT '0.00000' COMMENT '联通下',
  `lt_pause_d` bigint(10) DEFAULT '0' COMMENT '联通停顿总时长',
  `lt_pause_t` bigint(7) DEFAULT '0' COMMENT '联通停顿次数',
  `lt_b_samples` int(7) DEFAULT '0' COMMENT '联通忙时样本数',
  `lt_b_duration` bigint(10) DEFAULT '0' COMMENT '联通忙时收看总时长',
  `lt_b_download` float(9,5) DEFAULT '0.00000' COMMENT '联通忙时下',
  `lt_b_pause_d` bigint(10) DEFAULT '0' COMMENT '联通忙时停顿总时长',
  `lt_b_pause_t` bigint(7) DEFAULT '0' COMMENT '联通忙时停顿次数',
  `lt_i_samples` int(7) DEFAULT '0' COMMENT '联通闲时样本数',
  `lt_i_duration` bigint(10) DEFAULT '0' COMMENT '联通闲时收看总时长',
  `lt_i_download` float(9,5) DEFAULT '0.00000' COMMENT '联通闲时下',
  `lt_i_pause_d` bigint(10) DEFAULT '0' COMMENT '联通闲时停顿总时长',
  `lt_i_pause_t` bigint(7) DEFAULT '0' COMMENT '联通闲时停顿次数',
  `dx_samples` int(7) DEFAULT '0' COMMENT '电信样本数',
  `dx_duration` bigint(10) DEFAULT '0' COMMENT '电信收看总时长',
  `dx_download` float(9,5) DEFAULT '0.00000' COMMENT '电信下',
  `dx_pause_d` bigint(10) DEFAULT '0' COMMENT '电信停顿总时长',
  `dx_pause_t` bigint(7) DEFAULT '0' COMMENT '电信停顿次数',
  `dx_b_samples` int(7) DEFAULT '0' COMMENT '电信忙时样本数',
  `dx_b_duration` bigint(10) DEFAULT '0' COMMENT '电信忙时收看总时长',
  `dx_b_download` float(9,5) DEFAULT '0.00000' COMMENT '电信忙时下',
  `dx_b_pause_d` bigint(10) DEFAULT '0' COMMENT '电信忙时停顿总时长',
  `dx_b_pause_t` bigint(7) DEFAULT '0' COMMENT '电信忙时停顿次数',
  `dx_i_samples` int(7) DEFAULT '0' COMMENT '电信闲时样本数',
  `dx_i_duration` bigint(10) DEFAULT '0' COMMENT '电信闲时收看总时长',
  `dx_i_download` float(9,5) DEFAULT '0.00000' COMMENT '电信闲时下',
  `dx_i_pause_d` bigint(10) DEFAULT '0' COMMENT '电信闲时停顿总时长',
  `dx_i_pause_t` bigint(7) DEFAULT '0' COMMENT '电信闲时停顿次数',
  `known_as` varchar(8) DEFAULT '',
  `status` char(1) DEFAULT 'w',
  PRIMARY KEY (`addr`,`date`,`ent`),
  UNIQUE KEY `id` (`id`) USING BTREE,
  KEY `addr` (`addr`) USING BTREE,
  KEY `code` (`code`) USING BTREE,
  KEY `date` (`date`)
) ENGINE=InnoDB AUTO_INCREMENT=137497 DEFAULT CHARSET=utf8;
}
#最初的table

# noted by tuan tuan '099999990-ob ,mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm ,
# noted by tuan tuan l;p;l;l;'p
# noted by tuan tuan /';0[p-76666666t 
