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
    from bj_tj_sh where id=(select max(id) from bj_tj_sh)" -list]
puts $date
set datecode [string map {"-" ""} $date]
set YM [string range $datecode 0 5] ;#year and month
set day [string range $datecode 6 7] ;# date of a month
set entry [mysqlsel $mysql_handler "select * from data.download where
     ((data.download.addr='北京' or data.download.addr='北京市') or
     (data.download.addr='天津' or data.download.addr='天津市') or
     (data.download.addr='上海' or data.download.addr='上海市') or
     (data.download.addr='重庆' or data.download.addr='重庆市')) and
     data.download.YM=$YM and data.download.day=$day limit 1"]
if { $entry==0 } {
   mysqlexec $mysql_handler "insert into bj_tj_sh(YMD) values('$date')"
 } else {
   set query [ mysqlsel $mysql_handler "select ipaddr, addr,addr_2, addr_3,agency,date,max,avi,purpose, status,'$date'
     from data.download  where ((data.download.addr='北京' or data.download.addr='北京市') or 
     (data.download.addr='天津' or data.download.addr='天津市')or 
     (data.download.addr='上海' or data.download.addr='上海市') or
     (data.download.addr='重庆' or data.download.addr='重庆市')) and 
     data.download.YM=$YM and data.download.day=$day and (status='o' or status='i')" -list]
   foreach entry "$query" {
    set error [catch { mysqlexec $mysql_handler "insert into bj_tj_sh(ipaddr, addr,addr_2, addr_3,agency,date,max,avi,purpose,
     status,YMD) values('[lindex $entry 0]','[lindex $entry 1]','[lindex $entry 2]','[lindex $entry 3]','[lindex $entry 4]',
     '[lindex $entry 5]','[lindex $entry 6]','[lindex $entry 7]','[lindex $entry 8]','[lindex $entry 9]','[lindex $entry 10]')"} msg]
    if {$error } {
      puts $msg
    }
}}
mysqlclose $mysql_handler

