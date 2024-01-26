#!/bin/bash

source /home/aapostolakis/miniconda3/etc/profile.d/conda.sh

conda activate lazarus

modisCount=$1
meteoCount=$2

modisStartDate=$(date --date="$modisCount days ago" +'%Y%m%d')
meteoStartDate=$(date --date="$meteoCount days ago" +'%Y%m%d')
modisHashStartDate=$(date --date="$modisCount days ago" +'%Y-%m-%d')
meteoHashStartDate=$(date --date="$meteoCount days ago" +'%Y-%m-%d')
endDate=$(date +'%Y%m%d')
endHashDate=$(date +'%Y-%m-%d')
endDateForward=$(date -d "$endDate + 1 days" +"%Y%m%d")

python -u meteorological_copy.py $meteoStartDate $endDate /mnt/data3/beyondian/Stella/ /home/lstam/Documents/data/
#python -u meteorological_process.py $meteoStartDate $endDateForward /home/lstam/Documents/data/
python -u meteorological_process.py /home/lstam/Documents/data/

python -u mod11a2_myd11a2_download.py $modisHashStartDate $endHashDate /home/lstam/Documents/data/
python -u mod13a1_myd13a1_download.py $modisHashStartDate $endHashDate /home/lstam/Documents/data/

python -u mod11a2_myd11a2_process.py $modisStartDate $endDate /home/lstam/Documents/data/
python -u mod13a1_myd13a1_process.py $modisStartDate $endDate /home/lstam/Documents/data/

python -u mod11a2_myd11a2_merge.py $modisStartDate $endDate /home/lstam/Documents/data/
python -u mod13a1_myd13a1_merge.py $modisStartDate $endDate /home/lstam/Documents/data/

python -u unite.py $meteoStartDate $endDateForward

conda deactivate
