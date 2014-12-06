package require mysqltcl
global mysqlstatus
set port {3306}
set host {127.0.0.1}
set user {root}
set password {chinangi}
set dbname {webBrowsing} ;# calculated data
set data {data} ;#raw data
set mysql_handler [mysqlconnect -host $host -port $port -user $user -password $password]
set raw_data  [mysqlconnect -host $host -port $port -user $user -password $password]
set rowid 1
#起启数据库的行
mysqlexec $mysql_handler "set names 'utf8'"
mysqlexec $raw_data "set names 'utf8'"
mysqluse $mysql_handler $dbname
mysqluse $raw_data $data
#set YM "201409"
set sohuultime [mysqlsel $raw_data " select substring(filename,6,6) from webfiles where filename like 'sohu%' and 
  id=(select max(id) from webfiles where filename like'sohu%')" -list] 
set rightnow [mysqlsel $mysql_handler "select substring(now(),1,7)" -list]
set rightnow [string map {"-" ""} $rightnow]
if {$rightnow !=$sohuultime} {
  puts "Still need more data to upload"
  mysqlclose $mysql_handler
  mysqlclose $raw_data
  exit 1 
}
#if sohu not upload data of the previous month, waiting   
set YM [mysqlsel $raw_data "select substring(date,1,6) from webbrowsing where id=(select max(id) from webbrowsing where name='搜狐')" -list]
if {$YM==$rightnow} {
  puts "Waiting till the next month"
  mysqlclose $mysql_handler
  mysqlclose $raw_data
  exit 1
}
#already processed
set error [catch {mysqlexec $mysql_handler "insert firstscrn(enterprise,province, YM,carrier)
select distinct data.webbrowsing.name, data.webbrowsing.province, data.webbrowsing.prtcol, data.webbrowsing.carrier from data.webbrowsing
where data.webbrowsing.prtcol='$YM' and (data.webbrowsing.status='i' or data.webbrowsing.status='b')"} msg]
if {$error } {
  puts $msg }
while {1} {
  set entryid [mysqlsel $mysql_handler "select id from firstscrn where status='n' order by rand() limit 1" -list]
  if {$entryid==""} {
    puts end
    break
  }
  mysqlexec $mysql_handler begin
  set query [mysqlsel $mysql_handler "select enterprise,province, YM,carrier from firstscrn
  where id=$entryid for update" -list]
  set result [mysqlsel $raw_data "select avg(1stscreen), count(1stscreen) from webbrowsing where name='[lindex $query 0 0]' and
    province='[lindex  $query 0 1]' and prtcol='[lindex $query 0 2]' and carrier='[lindex $query 0 3]' and status='b'" -list]
  if {[lindex $result 0 1]==0} {
     set b_avg 0
     set b_smpl 0
  } else {
     set b_avg [lindex $result 0 0]
     set b_smpl [lindex $result 0 1]
  }
  set result [mysqlsel $raw_data "select avg(1stscreen), count(1stscreen) from webbrowsing where name='[lindex $query 0 0]' and
    province='[lindex  $query 0 1]' and prtcol='[lindex $query 0 2]' and carrier='[lindex $query 0 3]' and status='i'" -list]
  if {[lindex $result 0 1]==0} {
     set i_avg 0
     set i_smpl 0
  } else {
     set i_avg [lindex $result 0 0]
     set i_smpl [lindex $result 0 1]
  }
  if {[lindex $query 0 1]=="" } {
     code=" "
  } else  {
   set code [mysqlsel $raw_data "select id from region where name like '[lindex $query 0 1]%' and level=1" -list]
   }
  set error [catch {mysqlexec $mysql_handler "update firstscrn set busy=$b_avg, busysample=$b_smpl, idle=$i_avg, idlesample=$i_smpl,status='c',code='$code'
     where id=$entryid"} msg]
  if {$error} {
    puts "insert error $msg" }
  mysqlexec $mysql_handler commit
}
mysqlclose $mysql_handler
mysqlclose $raw_data
exit 0
