export LANG=en_US.UTF-8
cd /root/datacalc/
#python ./entandcar.py 
python ./download_final.py
python ./tabsync.py

#for (( i = 0; i <= 5; i++));
# do 
    tclsh streamingdaily.tcl &
    sleep 1
# done;
   tclsh streamingdaily.tcl &
   sleep 1
   tclsh streamingdaily.tcl &
   sleep 1
   tclsh streamingdaily.tcl &
   sleep 1
   tclsh streamingdaily.tcl &
   sleep 1
tclsh streamingdaily.tcl 
tclsh streamingdaily_province.tcl
tclsh bjtjshcq_dl.tcl   
tclsh bjtjshcq_streaming.tcl 
# done;


