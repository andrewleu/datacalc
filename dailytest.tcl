package require mysqltcl
global mysqlstatus

set port {3306}
set host {127.0.0.1}
set user {ipv6bgp}
set password {ipv6}
set dbname {ipinformation} ;# calculated data
set data {data} ;#raw data
set province {provienceoutline}
set cities "province"

set mysql_handler [mysqlconnect -host $host -port $port -user $user -password $password]
set raw_data  [mysqlconnect -host $host -port $port -user $user -password $password]
set rowid 1
#起启数据库的行
mysqlexec $mysql_handler "set names 'utf8'"
mysqlexec $raw_data "set names 'utf8'"
mysqluse $mysql_handler $dbname
mysqluse $raw_data $data
set finish 0
set counter 1 
set date [mysqlsel $mysql_handler "select distinct date from region where date != '' and level<3  
	 and pid != '110000' and pid !='120000' and pid != '310000' and pid != '500000' 
         and id!='710000' and id!='810000' and id!='820000' " -list]
set len [llength $date]
if {$len ==2}  {
   set date [mysqlsel $mysql_handler "select max(date) from region where date != ''" -list]
} elseif { $len ==1 } {
   set date [mysqlsel $mysql_handler "select date_add(date, interval 1 day) from region where date != '' limit 1" -list]
} else { exit 2 }
puts $date
set datecode [string map {"-" ""} $date]
set YM [string range $datecode 0 5] ;#year and month
set day [string range $datecode 6 7] ;# date of a month
while { $finish !=1} {
	 
	 mysqlexec $mysql_handler "begin"
	 set query [mysqlsel  $mysql_handler "select  `name`, `id`,`pname`,`level`,`known_as` from region   where level=2 and date != '$date' 
	 and pid != '110000' and pid !='120000' and pid != '310000' and pid != '500000'  order by rand()
	 limit 1 for update " -list]
	
	 set addr [lindex $query 0 0]
	 set code [lindex $query 0 1]
	 set parent [lindex $query 0 2]
	 set degree [lindex $query 0 3]
	 mysqlexec $mysql_handler  "update region set date='$date' where id = '$code'"  
	 mysqlexec $mysql_handler "commit" 
	 if { $query =="" } {
	 	   set finish 1
	 	   puts "done"
	 	   continue
	 	  }
	 
	 	 set init_time [clock seconds]
                 set init_time [clock format $init_time -format  {%Y-%m-%d %H:%M:%S}]

	 	  puts "$counter $query $init_time"
	 	  incr counter
	 set q_addr [lindex $query 0 4]
 	 set q_addr $q_addr%
        mysqlexec $raw_data "begin" 
	 #yidong avg 
	 set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where YM='$YM' and day= $day  and  
	     agency = '移动' and addr_2 like '$q_addr'  " -list]
	 set stat1 [string map {"{}" "0"} $stat1]
   set ydsample [expr {[lindex $stat1 0 0]}]
   set ydavg 0
   if {$ydsample !=0} {
     	   		set ydavg [expr { ( [lindex $stat1 0 1])/$ydsample}] 
   	}
	 #yidong busy
	 set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where day= $day 
	    and status='b' and agency ='移动' and YM='$YM' and addr_2 like '$q_addr'  " -list]
	 set stat1 [string map {"{}" "0"} $stat1]
   set ydsample_b [expr {[lindex $stat1 0 0]}]
   set ydavg_b 0
   if {$ydsample_b !=0 } { 
   	set ydavg_b [expr { ( [lindex $stat1 0 1])/$ydsample_b}]
  }
   #yidong idle
   	 set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where day = $day 
	    and status='i' and agency ='移动' and YM='$YM' and addr_2 like '$q_addr'   " -list]
	 set stat1 [string map {"{}" "0"} $stat1]
   set ydsample_i [expr {[lindex $stat1 0 0]}]
   set ydavg_i 0
   if {$ydsample_i !=0 } {
   	set ydavg_i [expr { ( [lindex $stat1 0 1])/$ydsample_i}]
  }
   
   #unicom avg 
   set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where day = $day 
	    and  agency ='联通' and YM='$YM' and addr_2 like '$q_addr'   " -list]
	 set stat1 [string map {"{}" "0"} $stat1]
   set ltsample [expr {[lindex $stat1 0 0]}]
   set ltavg 0
   if {$ltsample!=0 } { set ltavg [expr { ( [lindex $stat1 0 1])/$ltsample}] }
   #unicom busy
   set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where day = $day 
	    and status='b' and agency ='联通' and YM='$YM' and addr_2 like '$q_addr'  " -list]
	 set stat1 [string map {"{}" "0"} $stat1]
   set ltsample_b [expr {[lindex $stat1 0 0]}]
   set ltavg_b 0
   if {$ltsample_b !=0} { set ltavg_b [expr { ( [lindex $stat1 0 1])/$ltsample_b}] }
   #unicom idle 
   set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where day = $day 
	    and status='i' and agency ='联通' and YM='$YM' and addr_2 like '$q_addr'  " -list]
	 set stat1 [string map {"{}" "0"} $stat1]
   set ltsample_i [expr {[lindex $stat1 0 0]}]
   set ltavg_i 0
   if {$ltsample_i !=0 } {
   	set ltavg_i [expr { ( [lindex $stat1 0 1])/$ltsample_i}] 
   	}
   #dianxin avg
   set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where day =$day
	    and  agency ='电信' and YM='$YM' and addr_2 like '$q_addr'   " -list]
	 set stat1 [string map {"{}" "0"} $stat1]
   set dxsample [expr {[lindex $stat1 0 0]}]
   set dxavg 0
   if {$dxsample!=0} {
   	   		set dxavg [expr { ( [lindex $stat1 0 1])/$dxsample}]
   }
   #dianxin busy
   set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where day = $day 
	    and status='b' and agency ='电信' and YM='$YM' and addr_2 like '$q_addr'  " -list]
	 set stat1 [string map {"{}" "0"} $stat1]
   set dxsample_b [expr {[lindex $stat1 0 0]}]
   set dxavg_b 0
   if {$dxsample_b != 0} {
     set dxavg_b [expr { ( [lindex $stat1 0 1])/$dxsample_b}] 
     }
   
   #dianxin idle 
   set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where day = $day
	    and status='i' and agency ='电信' and YM='$YM' and addr_2 like '$q_addr'  " -list]
	 set stat1 [string map {"{}" "0"} $stat1]
   set dxsample_i [expr {[lindex $stat1 0 0]}]
   set dxavg_i 0
   if {$dxsample_i !=0} {
   	set dxavg_i [expr { ( [lindex $stat1 0 1])/$dxsample_i}]
   } 
   mysqlexec $raw_data "commit"
   set whole_avg 0
   if {[expr {$ydsample+$ltsample+$dxsample}] !=0 } {
   	 set whole_avg [expr { ($ydavg*$ydsample+$ltavg*$ltsample+$dxavg*$dxsample)/($ydsample+$ltsample+$dxsample)}]
   }
   if { $whole_avg !=0 } {
   mysqlexec $mysql_handler "insert into speed_daily(addr,code,parent,degree,date,average, yidongsample,
   yidong,yidong_busy,yidong_busy_sample,yidong_idle,yidong_idle_sample,liantongsample,liantong,liantong_busy,liantong_busy_sample,
   liantong_idle,liantong_idle_sample,dianxinsample,dianxin,dianxin_busy,dianxin_busy_sample,dianxin_idle,dianxin_idle_sample) values
   ('$addr','$code','$parent','$degree','$date','$whole_avg',
   '$ydsample','$ydavg','$ydavg_b','$ydsample_b','$ydavg_i','$ydsample_i',
   '$ltsample','$ltavg','$ltavg_b','$ltsample_b','$ltavg_i','$ltsample_i',
   '$dxsample','$dxavg','$dxavg_b','$dxsample_b','$dxavg_i','$dxsample_i')"  
   }
 }
 # city is calculated, blow is about province
 foreach code {"110000" "120000" "310000" "500000"} { 
 	   # beijing, shanghai, tianjin, chongqing
     mysqlexec $mysql_handler "begin"
     set query [mysqlsel $mysql_handler "select `name`,`known_as`  from region where date!='$date'
     and id='$code' limit 1 for update" -list]
     if { $query ==""} {
        mysqlexec $mysql_handler "commit"
       continue}
     puts $query
     set province [lindex $query 0 0]
     set provience [lindex $query 0 1]
     set provience "$provience%"
     mysqlexec $mysql_handler "update region set date='$date' where id='$code' "
     mysqlexec $mysql_handler "commit"
     set init_time [clock seconds]
     set init_time [clock format $init_time -format  {%Y-%m-%d %H:%M:%S}]
     puts "$province $init_time"
 #yidong avg
     set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where YM='$YM' and day = $day
         and agency = '移动' and addr like '$provience' " -list]
     set stat1 [string map {"{}" "0"} $stat1]
     set ydsample [expr {[lindex $stat1 0 0]}]
  set ydavg 0
   if {$ydsample !=0} {
         
                     set ydavg [expr {  [lindex $stat1 0 1]/$ydsample}]
        }
 #yidong busy
   set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where YM='$YM' and day = $day
         and status='b' and agency = '移动' and addr like '$provience' " -list]
   set stat1 [string map {"{}" "0"} $stat1]
   set ydsample_b [expr {[lindex $stat1 0 0]}]
   set ydavg_b 0
   if {$ydsample_b !=0} {
               set ydavg_b [expr { ( [lindex $stat1 0 1])/$ydsample_b}]
        }
 #yidong idle
   set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where YM='$YM' and day = $day
         and status='i' and agency = '移动' and addr like '$provience' " -list]
   set stat1 [string map {"{}" "0"} $stat1]
   set ydsample_i [expr {[lindex $stat1 0 0]}]
   set ydavg_i 0
   if {$ydsample_i !=0} {
         
                     set ydavg_i [expr { ( [lindex $stat1 0 1])/$ydsample_i}]
        }
 #unicom avg
     set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where YM='$YM' and day = $day
         and agency = '联通' and addr like '$provience' " -list]
     set stat1 [string map {"{}" "0"} $stat1]
     set ltsample [expr {[lindex $stat1 0 0]}]
     set ltavg 0
   if {$ltsample !=0} {
         
                     set ltavg [expr { ( [lindex $stat1 0 1])/$ltsample}]
        }
 #unicom busy
  set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where YM='$YM' and day = $day
         and status='b' and agency = '联通' and addr like '$provience' " -list]
  set stat1 [string map {"{}" "0"} $stat1]
  set ltsample_b [expr {[lindex $stat1 0 0]}]
  set ltavg_b 0
   if {$ltsample_b !=0} {
         
                     set ltavg_b [expr { ( [lindex $stat1 0 1])/$ltsample_b}]
        }
 #Unicom idle
  set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where YM='$YM' and day = $day
         and status='i' and agency = '联通' and addr like '$provience' " -list]
  set stat1 [string map {"{}" "0"} $stat1]
  set ltsample_i [expr {[lindex $stat1 0 0]}]
  set ltavg_i 0
   if {$ltsample_i !=0} {
         
                     set ltavg_i [expr { ( [lindex $stat1 0 1])/$ltsample_i}]
        }
 #dianxin avg
  set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where YM='$YM' and day = $day
         and agency = '电信' and addr like '$provience' " -list]
  set stat1 [string map {"{}" "0"} $stat1]
  set dxsample [expr {[lindex $stat1 0 0]}]
  set dxavg 0
   if {$dxsample !=0} {
         
                     set dxavg [expr { ( [lindex $stat1 0 1])/$dxsample}]
        }
 #dianxin busy
  set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where YM='$YM' and day = $day
         and status='b' and agency = '电信' and addr like '$provience' " -list]
  set stat1 [string map {"{}" "0"} $stat1]
  set dxsample_b [expr {[lindex $stat1 0 0]}]
  set dxavg_b 0
   if {$dxsample_b !=0} {
         
                     set dxavg_b [expr { ( [lindex $stat1 0 1])/$dxsample_b}]
        }
 #dianxin idle
  set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where YM='$YM' and day = $day
         and status='i' and agency = '电信' and addr like '$provience' " -list]
  set stat1 [string map {"{}" "0"} $stat1]
  set dxsample_i [expr {[lindex $stat1 0 0]}]
  set dxavg_i 0
   if {$dxsample_i !=0} {
         
                     set dxavg_i [expr { ( [lindex $stat1 0 1])/$dxsample_i}]
   }
  set prvc_avg [expr { ($ydavg*$ydsample+$ltavg*$ltsample+$dxavg*$dxsample)/($ydsample+$ltsample+$dxsample)}]
  if { $prvc_avg !=0 } {
  mysqlexec $mysql_handler "insert into speed_daily (addr,code,parent,degree,date,average,
  yidongsample,yidong,yidong_busy,yidong_busy_sample,yidong_idle,yidong_idle_sample,
  liantongsample,liantong,liantong_busy,liantong_busy_sample,liantong_idle,liantong_idle_sample,
  dianxinsample,dianxin,dianxin_busy,dianxin_busy_sample,dianxin_idle,dianxin_idle_sample)
  values
   ('$province','$code',0,1,'$date','$prvc_avg',
   '$ydsample','$ydavg','$ydavg_b','$ydsample_b','$ydavg_i','$ydsample_i',
   '$ltsample','$ltavg','$ltavg_b','$ltsample_b','$ltavg_i','$ltsample_i',
   '$dxsample','$dxavg','$dxavg_b','$dxsample_b','$dxavg_i','$dxsample_i')"  
   }
}

set finish  0
while {$finish!=1}   {
      mysqlexec $mysql_handler "begin"
      set query [mysqlsel  $mysql_handler "select `id`, `name` from region  
      where level=1 and date != '$date'  and id != '110000' and id !='120000' and id != '310000'
      and id != '500000' and id !='710000' and id !='810000' and id !='820000'  order by rand()
      limit 1 for update " -list]
    if { $query==""} {
         mysqlexec $mysql_handler "commit"
         set finish 1
         continue
       }
    set code [lindex  $query 0 0]
    set prvc [lindex $query 0 1]
 
     set init_time [clock seconds]
     set init_time [clock format $init_time -format  {%Y-%m-%d %H:%M:%S}]
     puts "$prvc $init_time"
    mysqlexec $mysql_handler "update region set date='$date' where id='$code'" 
    mysqlexec $mysql_handler "commit"
    #yidong
    set stat [mysqlsel $mysql_handler "select
        sum(yidongsample),sum(yidongsample*yidong),
        sum(yidong_busy_sample),sum(yidong_busy*yidong_busy_sample),
        sum(yidong_idle_sample), sum(yidong_idle*yidong_idle_sample)
      from speed_daily where parent='$prvc' and date='$date'" -list]
    puts  "$stat"
    set ydsample [lindex  $stat 0 0]
    set ydavg [expr {[lindex  $stat 0 1]/$ydsample}]
    set ydsample_b [lindex  $stat 0 2]
    if {$ydsample_b ==0 } {
      set ydavg_b 0 
    } else {
      set ydavg_b [expr {[lindex  $stat 0 3]/$ydsample_b}]
    }
    set ydsample_i [lindex  $stat 0 4]
    if {$ydsample_i == 0} {
       set ydavg_i 0
    } else {  
       set ydavg_i [expr {[lindex  $stat 0 5]/$ydsample_i}]
    }
    #liantong
    set stat [mysqlsel $mysql_handler "select
       sum(liantongsample),sum(liantongsample*liantong),
        sum(liantong_busy_sample),sum(liantong_busy*liantong_busy_sample),
        sum(liantong_idle_sample), sum(liantong_idle*liantong_idle_sample)
      from speed_daily where parent='$prvc' and date='$date'" -list]
    set ltsample [lindex  $stat 0 0]
    if { $ltsample == 0} {
        set ltavg 0 
    } else {
    set ltavg [expr {[lindex  $stat 0 1]/$ltsample}]
    }
    set ltsample_b [lindex  $stat 0 2]
    if {$ltsample_b ==0 } {
      set ltavg_b 0
    } else {
    set ltavg_b [expr {[lindex  $stat 0 3]/$ltsample_b}]
   } 
    set ltsample_i [lindex  $stat 0 4]
    if {$ltsample_i == 0} {
      set ltavg_i 0
    } else {
    set ltavg_i [expr {[lindex  $stat 0 5]/$ltsample_i}]
    }
    #dianxin
  
    set stat [mysqlsel $mysql_handler "select
       sum(dianxinsample),sum(dianxinsample*dianxin),
        sum(dianxin_busy_sample),sum(dianxin_busy*dianxin_busy_sample),
        sum(dianxin_idle_sample), sum(dianxin_idle*dianxin_idle_sample)
      from speed_daily where parent='$prvc' and date='$date'" -list]
    set dxsample [lindex  $stat 0 0]
    if { $dxsample ==0 } {
      set dxavg 0
    } else {
    set dxavg [expr {[lindex  $stat 0 1]/$dxsample}] }
    set dxsample_b [lindex  $stat 0 2]
    if {$dxsample_b == 0 } {
      set dxavg_b 0
   } else {
    set dxavg_b [expr {[lindex  $stat 0 3]/$dxsample_b}] }
    set dxsample_i [lindex  $stat 0 4]
    if {$dxsample_i ==0 } {
      set dxavg_i 0 
  } else {  
    set dxavg_i [expr {[lindex  $stat 0 5]/$dxsample_i}] }
    
    set prvc_avg [expr { ($ydavg*$ydsample+$ltavg*$ltsample+$dxavg*$dxsample)/
       ($ydsample+$ltsample+$dxsample)}]
   
   
     mysqlexec $mysql_handler "insert into speed_daily(addr,code,parent,degree,date,average,
        yidongsample,yidong,yidong_busy,yidong_busy_sample,yidong_idle,yidong_idle_sample,
        liantongsample,liantong,liantong_busy,liantong_busy_sample,liantong_idle,liantong_idle_sample,
        dianxinsample,dianxin,dianxin_busy,dianxin_busy_sample,dianxin_idle,dianxin_idle_sample)
     values
        ('$prvc','$code','0',1,'$date','$prvc_avg',
        '$ydsample','$ydavg','$ydavg_b','$ydsample_b','$ydavg_i','$ydsample_i',
        '$ltsample','$ltavg','$ltavg_b','$ltsample_b','$ltavg_i','$ltsample_i',
        '$dxsample','$dxavg','$dxavg_b','$dxsample_b','$dxavg_i','$dxsample_i')"
}   
 

  
 
 mysqlclose $mysql_handler
 mysqlclose $raw_data





# noted by tuan tuan '099999990-ob ,mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm ,
# noted by tuan tuan l;p;l;l;'p
# noted by tuan tuan /';0[p-76666666t 


