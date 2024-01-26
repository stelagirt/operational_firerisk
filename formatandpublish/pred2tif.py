'''
pred2tif.py
Usage: python -u pred2tif.py <prediction csv full path>
Creates:
- α GeoTIFF with decimal prediction
- α GeoTIFF with level prediction (1-5)
'''

import sys
import os
import xarray as xr
import pandas as pd
import geopandas as gpd
from datetime import datetime
from datetime import timedelta
import traceback
import re
import subprocess
import fileutils
from crop_dataset_files import cropfile

'''
Creates tif from CSV with decimal (0-1) and level (1-5) prediction
'''
def createtif(inputfile, pdate):
    '''tif files path and names'''
    bname=os.path.basename(inputfile)
    g = re.search('^(.*?)_pred', bname)
    rname=g.group(1)
    mainfolder = os.path.dirname(os.path.dirname(os.path.dirname((inputfile))))
    outfolder=os.path.join(mainfolder, 'tif', pdate)
    if not os.path.exists(outfolder): os.makedirs(outfolder)
    pubf=os.path.join(outfolder, 'published.txt')
    if os.path.isfile(pubf):
        print('Already published.')
        return
    friskshp = os.path.join(mainfolder, 'shp', rname + '_points.shp')
    ftifpred = os.path.join(outfolder, rname + '_prob.tif')
    ftifrisk = os.path.join(outfolder, rname + '_cells.tif')
    if os.path.isfile(ftifrisk) and os.path.isfile(ftifpred):
        print('Tiff files are ready : \n%s\n%s'%(ftifrisk,ftifpred))
        return

    #ftifpredfin = os.path.join(outfolder, rname + '_prob.tif')
    #ftifriskfin = os.path.join(outfolder, rname + '_cells.tif')
    #if os.path.isfile(ftifpredfin): os.remove(ftifpredfin)
    #if os.path.isfile(ftifriskfin): os.remove(ftifriskfin)

    try:
        '''read csv prediction file'''
        df = pd.read_csv(inputfile)
        '''convert dataframe to geodataframe with point geometry'''
        print('Convert csv to shp points, input: %s, output: %s'%(inputfile, friskshp))
        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['x'], df['y'], z=None, crs=4326))
        '''create shape file from geopandas df'''
        gdf.to_file(friskshp)
        '''get parameters for gdal_rasterize'''
        #gdf=gpd.read_file(friskshp)
        bounds=gdf.total_bounds
        df.set_index(["y", "x"], inplace=True)
        ds = xr.Dataset.from_dataframe(df)
        gdalparams=['gdal_rasterize', \
                    '-te', '%s' % bounds[0], '%s' % bounds[1], '%s' % bounds[2], '%s' % bounds[3], \
                    '-ts', '%s' % ds.dims['x'], '%s'%ds.dims['y'], '-a']
        '''Create two risk tif. "risk" is the risk level column, "ypred1" is the fire likelihood column'''
        print('Create tif, input: %s, output: %s'%(friskshp, ftifrisk))
        ex1 = subprocess.run(gdalparams + ['risk', friskshp, ftifrisk])
        print('Create tif, input: %s, output: %s' % (friskshp, ftifpred))
        ex2 = subprocess.run(gdalparams + ['ypred1', friskshp, ftifpred])

        '''crop to greece borders'''
        '''
        gdalparams2=['gdalwarp', '-cutline', 'perif/periphereies.shp',
                     '-crop_to_cutline', '-dstalpha']
        print('Clip to Greece Borders, input: %s, output: %s'%(ftifrisk, ftifriskfin))
        ex3 = subprocess.run(gdalparams2 + [ftifrisk, ftifriskfin])
        print('Clip to Greece Borders, input: %s, output: %s' % (ftifpred, ftifpredfin))
        ex4 = subprocess.run(gdalparams2 + [ftifpred, ftifpredfin])
        '''

        '''erase files'''
        for f in fileutils.find_files(os.path.dirname(friskshp), rname+'*', listtype="list"):
            os.remove(f)
        #os.remove(ftifrisk)
        #os.remove(ftifpred)
    except:
        print('Failed to produce tifs (%s, %s)\n' % (ftifpred, ftifrisk) + traceback.format_exc())

def main(args):

    inputdir = '/home/lstam/Documents/data/daily_rasters/csv'
    if len(args)!=1:
        nextday = datetime.now()+ timedelta(days=1)
        pdate = nextday.strftime("%Y%m%d")
    else:
        pdate = args[0]
    inputfile = os.path.join(inputdir,pdate,pdate+'_pred.csv')
    if not os.path.isfile(inputfile):
        print('Prediction csv not found: %s'%inputfile)
        return

    print('Crop to Greece borders')
    gdfperif = gpd.read_file('perif/periphereies.shp', encoding='Windows-1253')
    gdfperif = gdfperif.to_crs(4326)
    cropped=cropfile(inputfile, os.path.join(inputdir,pdate), gdfperif,'_greece')

    print('Create GeoTiff format files from prediction csv')
    createtif(cropped,pdate)

'''
Creates tif from CSV with decimal (0-1) and level (1-5) prediction using xarray.
--- Not working, shifts y coordinates --- 
'''
def createtif_bad(predf, inputfile):
    '''tif files path and names'''
    ftifpred = os.path.join(os.path.dirname(inputfile), 'tif', os.path.basename(predf).split('.')[0] + '_prob.tif')
    ftifrisk = os.path.join(os.path.dirname(inputfile), 'tif', os.path.basename(predf).split('.')[0] + '_risklev.tif')
    try:
        '''read csv prediction file'''
        df = pd.read_csv(predf)
        '''convert csv to xarray'''
        df.set_index(["y", "x"], inplace=True)
        ds = xr.Dataset.from_dataframe(df)
        ds.rio.write_crs(4326, inplace=True)
        '''xarray to tif (0-1)'''
        ds['ypred1'].rio.to_raster(ftifpred)
        '''xarray to tif (level 1-5)'''
        ds['risk'].rio.to_raster(ftifrisk)
    except:
        print('Failed to create tifs (%s, %s)\n' % (ftifpred, ftifrisk) + traceback.format_exc())
        return None


if __name__ == '__main__':
    main(sys.argv[1:])
