#application path
applpath="/home/lstam/Documents/newriskmodel"
# rename previous log
cd=$(date +"%Y%m%d")
one_day=$(date -d '-1 days' +"%Y%m%d")
file_time=$(date -r "$applpath/logs/totalflow.log" +"%Y%m%d")
if (( file_time <= one_day )); then
  echo "Renaming log"
  mv "$applpath/logs/totalflow.log" "$applpath/logs/totalflow$cd.log"
fi

