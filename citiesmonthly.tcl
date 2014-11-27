package require mysqltcl
global mysqlstatus

set port {3306}
set host {127.0.0.1}
set user {ipv6bgp}
set password {ipv6}
set dbname {ipinformation}
set data {data}
set province {provienceoutline}
set cities "province"
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
set raw_data  [mysqlconnect -host $host -port $port -user $user -password $password]
set rowid 1
#起启数据库的行
mysqlexec $mysql_handler "set names 'utf8'"
mysqlexec $raw_data "set names 'utf8'"
mysqluse $mysql_handler $dbname
mysqluse $raw_data $data
set finish 0
set counter 1 
while { $finish !=1} {
	 
	 mysqlexec $mysql_handler "begin"
	 set query [mysqlsel  $mysql_handler "select  `name`, `id`,`pname`,`level`,`known_as` from region   where level=2 and status !=$month 
	 and pid != '110000' and pid !='120000' and pid != '310000' and pid != '500000'  order by rand()
	 limit 1 for update " -list]
	
	 set addr [lindex $query 0 0]
	 set code [lindex $query 0 1]
	 set parent [lindex $query 0 2]
	 set degree [lindex $query 0 3]
	 mysqlexec $mysql_handler  "update region set status='$month' where id = '$code'"  
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
	 set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where YM='$yandm' 
	    and agency like '移动%' and addr_2 like '$q_addr'  " -list]
	 set stat1 [string map {"{}" "0"} $stat1]
   set ydsample [expr {[lindex $stat1 0 0]}]
  set ydavg 0
   if {$ydsample !=0} {
   	  
   	   		set ydavg [expr { ( [lindex $stat1 0 1])/$ydsample}] 
   	}
	 #yidong busy
	 set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where YM='$yandm' 
	    and status='b' and agency like '移动%' and addr_2 like '$q_addr'  " -list]
	 set stat1 [string map {"{}" "0"} $stat1]
   set ydsample_b [expr {[lindex $stat1 0 0]}]
   set ydavg_b 0
   if {$ydsample_b !=0 } { 
   	set ydavg_b [expr { ( [lindex $stat1 0 1])/$ydsample_b}]
  }
   #yidong idle
   	 set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where YM='$yandm' 
	    and status='i' and agency like '移动%' and addr_2 like '$q_addr'   " -list]
	 set stat1 [string map {"{}" "0"} $stat1]
   set ydsample_i [expr {[lindex $stat1 0 0]}]
   set ydavg_i 0
   if {$ydsample_i !=0 } {
   	set ydavg_i [expr { ( [lindex $stat1 0 1])/$ydsample_i}]
  }
   
   #unicom avg 
   set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where YM='$yandm' 
	    and  agency like '联通%' and addr_2 like '$q_addr'   " -list]
	 set stat1 [string map {"{}" "0"} $stat1]
   set ltsample [expr {[lindex $stat1 0 0]}]
   set ltavg 0
   if {$ltsample!=0 } { set ltavg [expr { ( [lindex $stat1 0 1])/$ltsample}] }
   #unicom busy
   set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where YM='$yandm' 
	    and status='b' and agency like '联通%' and addr_2 like '$q_addr'  " -list]
	 set stat1 [string map {"{}" "0"} $stat1]
   set ltsample_b [expr {[lindex $stat1 0 0]}]
   set ltavg_b 0
   if {$ltsample_b !=0} { set ltavg_b [expr { ( [lindex $stat1 0 1])/$ltsample_b}] }
   #unicom idle 
   set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where YM='$yandm' 
	    and status='i' and agency like '联通%' and addr_2 like '$q_addr'  " -list]
	 set stat1 [string map {"{}" "0"} $stat1]
   set ltsample_i [expr {[lindex $stat1 0 0]}]
   set ltavg_i 0
   if {$ltsample_i !=0 } {
   	set ltavg_i [expr { ( [lindex $stat1 0 1])/$ltsample_i}] 
   	}
   #dianxin avg
   set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where YM='$yandm' 
	    and  agency like '电信%' and addr_2 like '$q_addr'   " -list]
	 set stat1 [string map {"{}" "0"} $stat1]
   set dxsample [expr {[lindex $stat1 0 0]}]
   set dxavg 0
   if {$dxsample!=0} {
   	   		set dxavg [expr { ( [lindex $stat1 0 1])/$dxsample}]
   }
   #dianxin busy
   set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where YM='$yandm' 
	    and status='b' and agency like '电信%' and addr_2 like '$q_addr'  " -list]
	 set stat1 [string map {"{}" "0"} $stat1]
   set dxsample_b [expr {[lindex $stat1 0 0]}]
   set dxavg_b 0
   if {$dxsample_b != 0} {
     set dxavg_b [expr { ( [lindex $stat1 0 1])/$dxsample_b}] 
     }
   
   #dianxin idle 
   set stat1 [mysqlsel $raw_data "select count(id),sum(avi) from download where YM='$yandm' 
	    and status='i' and agency like '电信%' and addr_2 like '$q_addr'  " -list]
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

   mysqlexec $mysql_handler "insert into provience(addr,code,parent,degree,year,month,average, yidongsample,
   yidong,yidong_busy,yidong_busy_sample,yidong_idle,yidong_idle_sample,liantongsample,liantong,liantong_busy,liantong_busy_sample,
   liantong_idle,liantong_idle_sample,dianxinsample,dianxin,dianxin_busy,dianxin_busy_sample,dianxin_idle,dianxin_idle_sample) values
   ('$addr','$code','$parent','$degree','$year','$month','$whole_avg',
   '$ydsample','$ydavg','$ydavg_b','$ydsample_b','$ydavg_i','$ydsample_i',
   '$ltsample','$ltavg','$ltavg_b','$ltsample_b','$ltavg_i','$ltsample_i',
   '$dxsample','$dxavg','$dxavg_b','$dxsample_b','$dxavg_i','$dxsample_i')"
 }
 mysqlclose $mysql_handler
 mysqlclose $raw_data





# noted by tuan tuan '099999990-ob ,mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm ,
# noted by tuan tuan l;p;l;l;'p
# noted by tuan tuan /';0[p-76666666t 

