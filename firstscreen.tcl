package require mysqltcl
global mysqlstatus
set port {3306}
set host {127.0.0.1}
set user {ipv6bgp}
set password {ipv6}
set dbname {ipinformation} ;# calculated data
set data {data} ;#raw data
set mysql_handler [mysqlconnect -host $host -port $port -user $user -password $password]
set raw_data  [mysqlconnect -host "map" -port "3306" -user "root" -password "rtnet"]
set rowid 1
#起启数据库的行
mysqlexec $mysql_handler "set names 'utf8'"
mysqlexec $raw_data "set names 'utf8'"
mysqluse $mysql_handler $dbname
mysqluse $raw_data $data
set company [list "腾讯" "网易" "搜狐"]
set init_time [clock seconds]
set init_time [clock format $init_time -format  {%Y-%m-%d }]
set init_time [string range $init_time 0 7]
append init_time 01
set date [mysqlsel $mysql_handler "select date_sub('$init_time', interval 1 day) " -list]
set YM [string map {"-" ""} $date]
set YM [string range $YM 0 5]
foreach name $company {
  if {[mysqlsel $mysql_handler "select id from firstscrn where YM=$YM and enterprise='$name' limit 1"] } {
     continue
  } elseif { [mysqlsel $raw_data "select distinct name,province,prtcol,carrier from webbrowsing where prtcol='$YM'
  and (status='i' or status='b') and province <>'' and name='$name' limit 1 " ] } {
    puts "$name to process"
    incr count 
    set results [mysqlsel $raw_data "select distinct name,province,prtcol,carrier from webbrowsing 
    where prtcol='$YM' and (status='i' or status='b') and province <>'' and name='$name' " -list]
    set i 0
    foreach rslt "$results" {
      set r1 [lindex $rslt 0];
      set r2 [lindex $rslt 1]; 
      set r3 [lindex $rslt 2];
      set r4 [lindex $rslt 3];
      set error [catch {mysqlexec $mysql_handler "insert firstscrn(enterprise,province, YM,carrier) 
      values('$r1','$r2','$r3','$r4')"} msg]
      if {$error} {
         puts $msg
      }
     incr i ;#puts $i 
   } 
}}
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
       set code  0
  } else  {
   set code [mysqlsel $raw_data "select id from region where name like '[string range [lindex $query 0 1] 0 1]%' and level=1" -list]
    if { $code =="" } {
       set code 1
    }  
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
