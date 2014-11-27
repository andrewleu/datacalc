package require mysqltcl
#proc daily_metro_proc {region  telco ent} {
#    global raw_data; global mysql_handler
#    global YM; global day
    #puts $region
#    set all [mysqlsel $raw_data "select count(id),sum(watching), avg(download),sum(buffer),sum(buffer_duration) from streaming
#         where YM='$YM' and day= $day and carrier = '$telco' and  province like '$region%' and name='$ent' " -list]
#    set all [string map {"{}" "0"} $all]
#    set busy [mysqlsel $raw_data "select count(id),sum(watching), avg(download),sum(buffer),sum(buffer_duration) from streaming
#         where YM='$YM' and day= $day and carrier = '$telco' and  province like '$region%' and status='b' and name='$ent' " -list]
#    set busy [string map {"{}" "0"} $busy]
#    set idle [mysqlsel $raw_data "select count(id),sum(watching), avg(download),sum(buffer),sum(buffer_duration) from streaming
#         where YM='$YM' and day= $day and carrier = '$telco' and  province like '$region%' and status='i' and name='$ent'" -list]
#    set idle [string map {"{}" "0"} $idle]
#    set result [concat $all $busy $idle]
#    #puts $result
#    return $result
#  }
proc daily_city_proc {region  carrier fullname ent} {
    global raw_data; global mysql_handler
    global YM; global day
    #puts $fullname
    #puts $YM ; puts $day
	set all {{ }}
    if {[string length $fullname] > 5 } {
	  #set all [mysqlsel $raw_data "select count(id),sum(watching), avg(download),sum(buffer),sum(buffer_duration) from streaming 
	  #   where YM=$YM and `day`=$day and `carrier`='$carrier' and `name`='$ent' and  city like '$region%'  " -list]
	  #set all [string map {"{}" "0"} $all]
      set busy [mysqlsel $raw_data "select count(id),sum(watching), avg(download),sum(buffer),sum(buffer_duration) from streaming 
	    where YM=$YM and `day`=$day  and  `carrier`='$carrier' and `name`='$ent' and  city like '$region%' and status='b' " -list]
      set busy [string map {"{}" "0"} $busy]
      set idle [mysqlsel $raw_data "select count(id),sum(watching), avg(download),sum(buffer),sum(buffer_duration) from streaming 
	    where YM=$YM and `day`=$day  and  `carrier`='$carrier' and `name`='$ent' and  city like '$region%' and status='i' " -list]
		set idle [string map {"{}" "0"} $idle]
     } else {
          #set all [mysqlsel $raw_data "select count(id),sum(watching), avg(download),sum(buffer),sum(buffer_duration) from streaming
          #   where YM=$YM and `day`=$day and `carrier`='$carrier' and `name`='$ent' and  city = '$fullname'  " -list]
          #set all [string map {"{}" "0"} $all]
          set busy [mysqlsel $raw_data "select count(id),sum(watching), avg(download),sum(buffer),sum(buffer_duration) from streaming
            where YM=$YM and `day`=$day  and  `carrier`='$carrier' and `name`='$ent' and  city = '$fullname' and status='b' " -list]
          set busy [string map {"{}" "0"} $busy]
          set idle [mysqlsel $raw_data "select count(id),sum(watching), avg(download),sum(buffer),sum(buffer_duration) from streaming
            where YM=$YM and `day`=$day  and  `carrier`='$carrier' and `name`='$ent' and  city = '$fullname' and status='i' " -list]
          set idle [string map {"{}" "0"} $idle]
    }
    set result [concat $all $busy $idle]
    #puts $result
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
# if the table is completed or not
set remain [mysqlsel $mysql_handler "select count(id) from streaming_daily where status='w'" -list]
if {$remain==0} { 
#if completed, insert new entries
	set date [mysqlsel $mysql_handler "select date_add(date, interval 1 day) from streaming_daily where id=(select max(id) from streaming_daily)" -list]
	set error [catch {mysqlexec $mysql_handler "insert into streaming_daily(ent, addr,code, parent, date,known_as) 
        select 'Tencent',region.name, region.id,region.pid,'$date', region.known_as from region
	    where region.level <3 " } msg ]
    set error [catch {mysqlexec $mysql_handler "insert into streaming_daily(ent, addr,code, parent, date,known_as) 
        select '优酷',region.name, region.id,region.pid,'$date', region.known_as from region
	    where region.level <3 " } msg ]
    if {$error!=""} {puts $msg}
}
set date [mysqlsel $mysql_handler "select distinct date from streaming_daily  
    where status='w'  " -list]
puts $date
set datecode [string map {"-" ""} $date]
set YM [string range $datecode 0 5] ;#year and month
if {[string range $datecode 6 6] == "0" } {
   set day [string range $datecode 7 7]
} else {
  set day [string range $datecode 6 7] ;# date of a month
}
#set day [expr {$day+0}]
set finish 0
set r [expr rand()]
puts $r
after [expr int($r*1000000)%20000]
while { $finish !=1} {
	 set query [mysqlsel  $mysql_handler "select `id`, `ent`,`addr`,`code`, `known_as` from streaming_daily 
	   where (parent!=0   and status='w' )   order by rand() limit 1  " -list]
     if {$query ==""} {
          set finish 1; puts "city finished"; 
          mysqlexec $mysql_handler "commit";
          continue
        }
     mysqlexec $mysql_handler "begin"
     set error [catch {set query [mysqlsel $mysql_handler "select `id`, `addr`,`code`, `known_as`,`date`,`ent` from streaming_daily 
          where id=[lindex $query 0 0] for update " -list] } msg]
     if {$error } {continue}
           #addr=[lindex $query 0 1] and date=[lindex $query 0 4] for update" -list]
     set result_yd [daily_city_proc [lindex $query 0 3] "移动" [lindex $query 0 1] [lindex $query 0 5]]
     set result_lt [daily_city_proc [lindex $query 0 3] "联通" [lindex $query 0 1] [lindex $query 0 5]]
     set result_dx [daily_city_proc [lindex $query 0 3] "电信" [lindex $query 0 1] [lindex $query 0 5]]
	 mysqlexec $mysql_handler "update streaming_daily set  
	 yd_b_samples=[lindex $result_yd 1 0], 	 yd_b_duration =[lindex $result_yd 1 1], yd_b_download=[lindex $result_yd 1 2],
	 yd_b_pause_t=[lindex $result_yd 1 3], yd_b_pause_d=[lindex $result_yd 1 4],
	 yd_i_samples=[lindex $result_yd 2 0], 	 yd_i_duration =[lindex $result_yd 2 1], yd_i_download=[lindex $result_yd 2 2],
	 yd_i_pause_t=[lindex $result_yd 2 3], yd_i_pause_d=[lindex $result_yd 2 4],
	 lt_b_samples=[lindex $result_lt 1 0], 	 lt_b_duration =[lindex $result_lt 1 1], lt_b_download=[lindex $result_lt 1 2],
	 lt_b_pause_t=[lindex $result_lt 1 3], lt_b_pause_d=[lindex $result_lt 1 4],
	 lt_i_samples=[lindex $result_lt 2 0], 	 lt_i_duration =[lindex $result_lt 2 1], lt_i_download=[lindex $result_lt 2 2],
	 lt_i_pause_t=[lindex $result_lt 2 3], lt_i_pause_d=[lindex $result_lt 2 4],
	 dx_b_samples=[lindex $result_dx 1 0], 	 dx_b_duration =[lindex $result_dx 1 1], dx_b_download=[lindex $result_dx 1 2],
	 dx_b_pause_t=[lindex $result_dx 1 3], dx_b_pause_d=[lindex $result_dx 1 4],
	 dx_i_samples=[lindex $result_dx 2 0], 	 dx_i_duration =[lindex $result_dx 2 1], dx_i_download=[lindex $result_dx 2 2],
	 dx_i_pause_t=[lindex $result_dx 2 3], dx_i_pause_d=[lindex $result_dx 2 4], status='c'
	 where id=[lindex $query 0 0]" 
	mysqlexec $mysql_handler "commit"
   }
 # city is calculated, blow is about province
 after [expr int(rand()*20000)]



mysqlclose $mysql_handler
mysqlclose $raw_data


