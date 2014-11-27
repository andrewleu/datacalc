package require mysqltcl
global mysqlstatus
 
set port {3306}
set host {127.0.0.1}
set user {ipv6bgp}
set password {ipv6}
set dbname {ipinformation}
set data {data}
set province {provienceoutline}
set year "2013"
set month "11"
if { [string length $month] >2 || $month >12} {
     puts "input correct month"
     exit 0
}
if {[string length $month]==2} {
set yandm $year$month
} else {
     set month 0$month
     set yandm $year$month
}
set mysql_handler [mysqlconnect -host $host -port $port -user $user -password $password]
set raw_data [mysqlconnect -host $host -port $port -user $user -password $password]
mysqlexec $mysql_handler "set names 'utf8'"
mysqlexec $raw_data "set names 'utf8'"
mysqluse $raw_data "data"
set rowid 1
#起启数据库的行
 
 
 
#set code 110000
foreach code {"110000" "120000" "310000" "500000"} {
     mysqluse $mysql_handler "$dbname"
     mysqlexec $mysql_handler "begin"
     set query [mysqlsel $mysql_handler "select `name`,`known_as`  from region where status!=$month
     and id='$code' limit 1 for update" -list]
     if { $query ==""} {
        mysqlexec $mysql_handler "commit"
       continue}
     puts $query
     set province [lindex $query 0 0]
     set provience [lindex $query 0 1]
     set provience "$provience%"
     mysqlexec $mysql_handler "update region set status = '$month' where id='$code' "
     mysqlexec $mysql_handler "commit"
     mysqluse $mysql_handler "data"
     set init_time [clock seconds]
     set init_time [clock format $init_time -format  {%Y-%m-%d %H:%M:%S}]
     puts "$province $init_time"
 #yidong avg
     set stat1 [mysqlsel $mysql_handler "select count(id),sum(avi) from download where YM='$yandm'
         and agency = '移动' and addr like '$provience' " -list]
     set stat1 [string map {"{}" "0"} $stat1]
     set ydsample [expr {[lindex $stat1 0 0]}]
  set ydavg 0
   if {$ydsample !=0} {
         
                     set ydavg [expr {  [lindex $stat1 0 1]/$ydsample}]
        }
 #yidong busy
   set stat1 [mysqlsel $mysql_handler "select count(id),sum(avi) from download where YM='$yandm'
         and status='b' and agency = '移动' and addr like '$provience' " -list]
   set stat1 [string map {"{}" "0"} $stat1]
   set ydsample_b [expr {[lindex $stat1 0 0]}]
   set ydavg_b 0
   if {$ydsample_b !=0} {
         
                     set ydavg_b [expr { ( [lindex $stat1 0 1])/$ydsample_b}]
        }
 #yidong idle
   set stat1 [mysqlsel $mysql_handler "select count(id),sum(avi) from download where YM='$yandm'
         and status='i' and agency = '移动' and addr like '$provience' " -list]
   set stat1 [string map {"{}" "0"} $stat1]
   set ydsample_i [expr {[lindex $stat1 0 0]}]
   set ydavg_i 0
   if {$ydsample_i !=0} {
         
                     set ydavg_i [expr { ( [lindex $stat1 0 1])/$ydsample_i}]
        }
 #unicom avg
     set stat1 [mysqlsel $mysql_handler "select count(id),sum(avi) from download where YM='$yandm'
         and agency = '联通' and addr like '$provience' " -list]
     set stat1 [string map {"{}" "0"} $stat1]
     set ltsample [expr {[lindex $stat1 0 0]}]
     set ltavg 0
   if {$ltsample !=0} {
         
                     set ltavg [expr { ( [lindex $stat1 0 1])/$ltsample}]
        }
 #unicom busy
  set stat1 [mysqlsel $mysql_handler "select count(id),sum(avi) from download where YM='$yandm'
         and status='b' and agency = '联通' and addr like '$provience' " -list]
  set stat1 [string map {"{}" "0"} $stat1]
  set ltsample_b [expr {[lindex $stat1 0 0]}]
  set ltavg_b 0
   if {$ltsample_b !=0} {
         
                     set ltavg_b [expr { ( [lindex $stat1 0 1])/$ltsample_b}]
        }
 #Unicom idle
  set stat1 [mysqlsel $mysql_handler "select count(id),sum(avi) from download where YM='$yandm'
         and status='i' and agency = '联通' and addr like '$provience' " -list]
  set stat1 [string map {"{}" "0"} $stat1]
  set ltsample_i [expr {[lindex $stat1 0 0]}]
  set ltavg_i 0
   if {$ltsample_i !=0} {
         
                     set ltavg_i [expr { ( [lindex $stat1 0 1])/$ltsample_i}]
        }
 #dianxin avg
  set stat1 [mysqlsel $mysql_handler "select count(id),sum(avi) from download where YM='$yandm'
         and agency = '电信' and addr like '$provience' " -list]
  set stat1 [string map {"{}" "0"} $stat1]
  set dxsample [expr {[lindex $stat1 0 0]}]
  set dxavg 0
   if {$dxsample !=0} {
         
                     set dxavg [expr { ( [lindex $stat1 0 1])/$dxsample}]
        }
 #dianxin busy
  set stat1 [mysqlsel $mysql_handler "select count(id),sum(avi) from download where YM='$yandm'
         and status='b' and agency = '电信' and addr like '$provience' " -list]
  set stat1 [string map {"{}" "0"} $stat1]
  set dxsample_b [expr {[lindex $stat1 0 0]}]
  set dxavg_b 0
   if {$dxsample_b !=0} {
         
                     set dxavg_b [expr { ( [lindex $stat1 0 1])/$dxsample_b}]
        }
 #dianxin idle
  set stat1 [mysqlsel $mysql_handler "select count(id),sum(avi) from download where YM='$yandm'
         and status='i' and agency = '电信' and addr like '$provience' " -list]
  set stat1 [string map {"{}" "0"} $stat1]
  set dxsample_i [expr {[lindex $stat1 0 0]}]
  set dxavg_i 0
   if {$dxsample_i !=0} {
         
                     set dxavg_i [expr { ( [lindex $stat1 0 1])/$dxsample_i}]
   }
  set prvc_avg [expr { ($ydavg*$ydsample+$ltavg*$ltsample+$dxavg*$dxsample)/($ydsample+$ltsample+$dxsample)}]
  mysqluse $mysql_handler $dbname
  mysqlexec $mysql_handler "insert into provienceoutline (provience,code,year,month,average,
  yidongsample,yidong,yidong_busy,yidong_busy_sample,yidong_idle,yidong_idle_sample,
  liantongsample,liantong,liantong_busy,liantong_busy_sample,liantong_idle,liantong_idle_sample,
  dianxinsample,dianxin,dianxin_busy,dianxin_busy_sample,dianxin_idle,dianxin_idle_sample)
  values
   ('$province','$code','$year','$month','$prvc_avg',
   '$ydsample','$ydavg','$ydavg_b','$ydsample_b','$ydavg_i','$ydsample_i',
   '$ltsample','$ltavg','$ltavg_b','$ltsample_b','$ltavg_i','$ltsample_i',
   '$dxsample','$dxavg','$dxavg_b','$dxsample_b','$dxavg_i','$dxsample_i')"
}
 mysqluse $mysql_handler $dbname
 set finish  0
 while {$finish!=1}   {
      mysqlexec $mysql_handler "begin"
      set query [mysqlsel  $mysql_handler "select `id`, `name` from region  
      where level=1 and status !=$month  and id != '110000' and id !='120000' and id != '310000'
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
    mysqlexec $mysql_handler "update region set status='$month' where id='$code'" 
    mysqlexec $mysql_handler "commit"
    #yidong
    set stat [mysqlsel $mysql_handler "select
        sum(yidongsample),sum(yidongsample*yidong),
        sum(yidong_busy_sample),sum(yidong_busy*yidong_busy_sample),
        sum(yidong_idle_sample), sum(yidong_idle*yidong_idle_sample)
      from provience where parent='$prvc' and year='$year' and month= $month" -list]
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
      from provience where parent='$prvc' and year='$year' and month= $month" -list]
    set ltsample [lindex  $stat 0 0]
    set ltavg [expr {[lindex  $stat 0 1]/$ltsample}]
    set ltsample_b [lindex  $stat 0 2]
    set ltavg_b [expr {[lindex  $stat 0 3]/$ltsample_b}]
    set ltsample_i [lindex  $stat 0 4]
    set ltavg_i [expr {[lindex  $stat 0 5]/$ltsample_i}]
   
    #dianxin
  
    set stat [mysqlsel $mysql_handler "select
       sum(dianxinsample),sum(dianxinsample*dianxin),
        sum(dianxin_busy_sample),sum(dianxin_busy*dianxin_busy_sample),
        sum(dianxin_idle_sample), sum(dianxin_idle*dianxin_idle_sample)
      from provience where parent='$prvc' and year='$year' and month= $month" -list]
    set dxsample [lindex  $stat 0 0]
    set dxavg [expr {[lindex  $stat 0 1]/$dxsample}]
    set dxsample_b [lindex  $stat 0 2]
    set dxavg_b [expr {[lindex  $stat 0 3]/$dxsample_b}]
    set dxsample_i [lindex  $stat 0 4]
    set dxavg_i [expr {[lindex  $stat 0 5]/$dxsample_i}]
    
    set prvc_avg [expr { ($ydavg*$ydsample+$ltavg*$ltsample+$dxavg*$dxsample)/
       ($ydsample+$ltsample+$dxsample)}]
   
   
     mysqlexec $mysql_handler "insert into provienceoutline (provience,code,year,month,average,
        yidongsample,yidong,yidong_busy,yidong_busy_sample,yidong_idle,yidong_idle_sample,
        liantongsample,liantong,liantong_busy,liantong_busy_sample,liantong_idle,liantong_idle_sample,
        dianxinsample,dianxin,dianxin_busy,dianxin_busy_sample,dianxin_idle,dianxin_idle_sample)
     values
        ('$prvc','$code','$year','$month','$prvc_avg',
        '$ydsample','$ydavg','$ydavg_b','$ydsample_b','$ydavg_i','$ydsample_i',
        '$ltsample','$ltavg','$ltavg_b','$ltsample_b','$ltavg_i','$ltsample_i',
        '$dxsample','$dxavg','$dxavg_b','$dxsample_b','$dxavg_i','$dxsample_i')"
}   
 
  foreach code {710000  810000 820000} {
    mysqluse $mysql_handler "ipinformation"
    mysqlexec $mysql_handler "begin"
    set query [ mysqlsel $mysql_handler "select `id`, `name`,`known_as` from region where id='$code'
    and status != '$month' for update" -list]
    mysqlexec $mysql_handler "update region set status='$month' where id='$code'"
    mysqlexec $mysql_handler "commit"
    puts "$query"
    if {$query == "" } {
       
        continue
         }
    set prvc [lindex $query 0 1]
    set q_addr [lindex $query 0 2]
    set q_addr "$q_addr%"
 
    set stat1 [mysqlsel $raw_data "select count(id), sum(avi) from download where YM='$yandm' and addr like '$q_addr' " -list]
    set stat1 [string map {"{}" "0"} $stat1]
    set avg [expr {([lindex $stat1 0 1])/
    ([lindex $stat1 0 0])}]
    mysqlexec $mysql_handler "insert into provienceoutline (provience,code,year,month,average)
     values  ('$prvc','$code','$year','$month','$avg')"
  }
 
 
 
mysqlclose $mysql_handler
mysqlclose $raw_data
 

