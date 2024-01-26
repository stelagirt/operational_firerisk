# Replaces risk map for specific dates
# Usage example:
#       replace_risk.sh 2023-8-1 2023-08-10

input_start=$1
input_end=$2

source /home/aapostolakis/miniconda3/etc/profile.d/conda.sh
applpath="/home/lstam/Documents/newriskmodel"

startdate=$(date -I -d "$input_start") || exit -1
enddate=$(date -I -d "$input_end")     || exit -1

d="$startdate"
while [ "$d" != "$enddate" ]; do 
  echo "Running for $d"

  # delete
  dt=$(date -d "$d" +%Y%m%d)

  echo "deleting from service platform /home/lstam/Documents/data/daily_rasters/tif/${dt}_cells.tif"
  cd "$applpath/formatandpublish"
  conda deactivate
  conda activate xarray
  python -u AddNewImagesToImageService_v2.py -f "/home/lstam/Documents/data/daily_rasters/tif/${dt}_cells.tif"  -m delete
  
  #echo "deleting folder /home/lstam/Documents/data/daily_rasters/csv/${dt}"
  #rm -R "/home/lstam/Documents/data/daily_rasters/csv/${dt}"
  #echo "deleting folder /home/lstam/Documents/data/daily_rasters/tif/${dt}"
  #rm -R "/home/lstam/Documents/data/daily_rasters/tif/${dt}"
  # run prediction
  #cd "$applpath/prediction"
  #conda deactivate
  #conda activate lazarus
  #python -u predict_tab.py $dt

  #format and publish
  cd "$applpath/formatandpublish"
  conda deactivate
  conda activate xarray
  #python -u pred2tif.py $dt
  python -u fireriskmapsend.py -d $dt -pm
  d=$(date -I -d "$d + 1 day")
done

