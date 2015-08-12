#to select speed data of the 4 major cities bj tj sh cq 
package require mysqltcl
global mysqlstatus

set port {3306}
set host {127.0.0.1}
set user {ipv6bgp}
set password {ipv6}
set dbname {ipinformation} ;# calculated data
set mysql_handler [mysqlconnect -host $host -port $port -user $user -password $password]
#起启数据库的行
mysqlexec $mysql_handler "set names 'utf8'"
mysqluse $mysql_handler $dbname
set date [mysqlsel $mysql_handler "select date_add(YMD, interval 1 day) 
    from bjtjshcqstreaming where id=(select max(id) from bjtjshcqstreaming)" -list]
puts $date
set datecode [string map {"-" ""} $date]
set YM [string range $datecode 0 5] ;#year and month
set day [string range $datecode 6 7] ;# date of a month
set entry [mysqlsel $mysql_handler "select * from data.streaming where 
     ((data.streaming.province='北京' or data.streaming.province='北京市') or
     (data.streaming.province='天津' or data.streaming.province='天津市')or
     (data.streaming.province='上海' or data.streaming.province='上海市') or
     (data.streaming.province='重庆' or data.streaming.province='重庆市')) and
     data.streaming.YM=$YM and data.streaming.day=$day limit 1"]
if {$entry==0} {
   mysqlexec $mysql_handler "insert into bjtjshcqstreaming(YMD) value('$date')"
} else { 
  set query [mysqlsel $mysql_handler "select name,ipaddr, province,city,county,carrier,timestamp,os,
     term,software,s_rate,s_duration,watching, download,buffer,buffer_duration,status, '$date'
     from data.streaming where ((data.streaming.province='北京' or data.streaming.province='北京市') or 
     (data.streaming.province='天津' or data.streaming.province='天津市')or 
     (data.streaming.province='上海' or data.streaming.province='上海市') or
     (data.streaming.province='重庆' or data.streaming.province='重庆市')) and 
     data.streaming.YM=$YM and data.streaming.day=$day and (status='b' or status='i')" -list]
  foreach entry "$query" {
  set error [catch {mysqlexec $mysql_handler "insert into bjtjshcqstreaming(name,ipaddr, province,city,county,carrier,timestamp,os,
  term,software,s_rate,s_duration,watching, download,buffer,buffer_duration,status, YMD) values('[lindex $entry 0]',
  '[lindex $entry 1]','[lindex $entry 2]','[lindex $entry 3]','[lindex $entry 4]','[lindex $entry 5]','[lindex $entry 6]',
  '[lindex $entry 7]','[lindex $entry 8]','[lindex $entry 9]','[lindex $entry 10]', '[lindex $entry 11]','[lindex $entry 12]',
  '[lindex $entry 13]','[lindex $entry 14]','[lindex $entry 15]','[lindex $entry 16]','[lindex $entry 17]')" } msg]
  if {$error} {
    puts $msg}
} }
mysqlclose $mysql_handler

