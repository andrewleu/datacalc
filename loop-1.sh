export LANG=en_US.UTF-8
cd /root/datacalc/
#python ./entandcar.py
#python ./tabsync.py

for (( i = 0; i < 3; i++));
 do
   python ./download_final.py
#   tclsh bjtjshcq_dl.tcl
#   tclsh bjtjshcq_streaming.tcl
   python stream_month_city.py
#   tclsh firstscreen.tcl
  sleep 1
 done;
#tclsh streamingdaily_province.tcla
