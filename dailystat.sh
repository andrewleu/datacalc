export LANG=en_US.UTF-8
cd /root/datacalc/
#python ./entandcar.py 
echo "download"
python ./download_final.py
#python ./tabsync.py

#for (( i = 0; i <= 5; i++));
# do 
#    tclsh streamingdaily.tcl &
#    sleep 1
# done;
#tclsh streamingdaily_province.tcla
echo "4 cities"
tclsh bjtjshcq_dl.tcl   
tclsh bjtjshcq_streaming.tcl 
# done;
echo "Firstscreen"
tclsh firstscreen.tcl
echo "Streaming monthly"
python stream_month_city.py

