'''
predict_tab.py
Usage: python -u predict_tab.py <netcdf full path>
Creates:
- an unnormalized csv from the netcdf dataset
- a normalized csv, the input to the model
- a csv with the prediction. csv columns: id, x, y, predictions for class 0 (0-1),
   prediction for class 1 (0-1, 1-pred0), level of risk (1-5)
- α GeoTIFF with decimal prediction
- α GeoTIFF with level prediction (1-5)
'''

import sys
import re
import normdataset
import os
import xarray as xr
import pandas as pd
from manage_model import mm_load_model, run_predict
from best_models import retrieve_best_models, retrieve_models_by_id
from check_and_prepare_dataset import load_dataset
import traceback
from datetime import datetime
import numpy as np


'''
preprocess functions: drop columns, fillna filters, renames, horizontal filters, add columns, calibration filters
'''
def preprocess(df, dropfilters=None, fillnafilters=None, renames=None, horizfilters=None, \
                  addcols=None, calibfilters=None):
    if fillnafilters is not None:
        fillnacols=[c for c in df.columns if any(re.search(p,c) for p in fillnafilters)]
        for c in fillnacols:
            df[c].fillna(0, inplace=True)
    #print('before drop %d',len(df))
    #print('after drop %d', len(df))
    if dropfilters is not None:
        dropcols=[c for c in df.columns if any(re.search(p,c) for p in dropfilters)]
        df.drop(columns=dropcols,inplace=True)
        #print('dropped cols %s, col num before: %d, col num after: %d'%(dropcols,colnb, len(df.columns)))
        #print('dropped cols %s, col num before: %d, col num after: %d'%(dropcols,colnb, len(df.columns)))
    if horizfilters is not None:
        for hf in horizfilters:
            cond=eval(hf)
            df=df.copy()[cond]
    df.dropna(inplace=True)
    if renames is not None:
        df.rename(columns=renames,inplace=True)
    if addcols is not None:
        for addc in addcols:
            df[addc]=0
    if calibfilters is not None:
        for calib in calibfilters:
            '''v is for the eval expression use'''
            v=df[calib]
            df[calib]=eval(calibfilters[calib])
            #df.drop(columns=[calib], inplace=True)
            #df.rename(columns={calib+'_temp':calib}, inplace=True)
    return df


'''
convert netcdf to tabular
'''
def netcdf_to_csv(ncname, statname, tfolder, \
                  dropfilters=None, fillnafilters=None, renames=None, horizfilters=None, \
                  addcols=None, calibfilters=None
                  ):
    bname = os.path.basename(ncname)
    g1 = re.search('^(.*?)\.nc', bname)
    csvname = os.path.join(tfolder, g1.group(1) + '.csv')
    if os.path.isfile(csvname):
        print('Found ready Unormalized CSV')
        return csvname
    try:
        ds=xr.open_dataset(ncname)
        ds_stat=xr.open_dataset(statname)
        dsdayall=xr.merge([ds,ds_stat],combine_attrs='drop')
        dfday=dsdayall.to_dataframe().reset_index()
        dfday = preprocess(dfday, dropfilters, fillnafilters, renames, horizfilters, \
                   addcols, calibfilters)
        dfday.to_csv(csvname, index=False)
        return csvname
    except:
        print('Failed to create unnormalized csv %s\n'%csvname+traceback.format_exc())
        return None

'''
normalize - preprocess
'''
def normalizefile(csvfile, aggrfile, dropfilters=None, fillnafilters=None, renames=None, horizfilters=None, \
                  addcols=None, calibfilters=None, applyid=True):

    f=csvfile
    bname = os.path.basename(f)
    g1 = re.search('^(.*?)\.csv', bname)
    fnorm = os.path.join(os.path.dirname(f), g1.group(1) + '_norm.csv')
    if os.path.isfile(fnorm):
        print('Found ready Normalized CSV')
        return fnorm
    try:
        #if f[-8:]=='norm.csv' or os.path.isfile(fnorm):
        #    print("file exists or wrong name")
        #    return
        #print('Start processing file %s'%f)
        df=pd.read_csv(f)
        #df=df.sample(frac=1)
        df.reset_index(inplace=True, drop=True)
        df = preprocess(df, dropfilters, fillnafilters, renames, horizfilters, \
                   addcols, calibfilters)
        '''Add ID. This ID is x,y with 4 decimals (6 + 6 digits) concatenated without space (all numbers)'''
        if applyid:
            df['xposst'] = (df['x'] * 10000).apply('{:06.0f}'.format)
            df['yposst'] = (df['y'] * 10000).apply('{:06.0f}'.format)
            df['id'] = df['xposst']+df['yposst']
            df.drop(columns=['xposst', 'yposst'],inplace=True)
        normdf = normdataset.normalize_dataset(df,\
                aggrfile=aggrfile, check=False)
        if 'firedate' not in normdf.columns:
            normdf['firedate']=os.path.basename(f)[0:8]
        normdf.to_csv(fnorm,index=False)
        #print('Done processing file. Output %s' % fnorm)
        return fnorm
    except:
        print('Failed to create normalized csv %s\n'%fnorm+traceback.format_exc())
        return None

'''get the model parameters form the folder of the experiments results'''
def get_model_params():
    filters = ["df_flt['params'].str.contains(\"'dropout': None\")"]  # no dropout
    # filters = ["~df_flt['params'].str.contains(\"'dropout': None\")"] # with dropout
    opt_targets = ['auc', 'f1-score 1', 'hybrid1', 'hybrid2', 'hybrid5', 'NH2', 'NH5', 'NH10']
    testfpattern = '*NN_ns*mean*'
    resdir = '/home/lstam/Documents/bestmodels'
    modelp = retrieve_models_by_id(785, 'hybrid2 test', resdir, testfpattern, opt_targets, 'val.', 'test', filters,3)
    return modelp['params']


'''run the selected model to produce predictions'''
def modelrun(datasetf, modelf):
    folder=os.path.dirname(datasetf)
    bname=os.path.basename(datasetf)
    g = re.search('^(.*?)_norm', bname)
    fpred = os.path.join(folder, g.group(1)+'_pred.csv')
    if os.path.isfile(fpred):
        print('Found prediction CSV ready')
        return fpred
    try:
        '''loading dataset '''
        modelparams = get_model_params()
        X_pd, y_pd, g = load_dataset(datasetf, modelparams['feature_drop'])
        '''align columns'''
        X_pd = X_pd.reindex(sorted(X_pd.columns), axis=1)
        '''get data arrays'''
        X = X_pd.values
        y = y_pd.values
        y = y[:, 0]
        '''load model and run prediction'''
        model=mm_load_model(modelf)
        y_scores, y_pred = run_predict(model,'tf',X)
        '''format and write output'''
        dset=pd.read_csv(datasetf)
        out = pd.concat([dset['id'], pd.DataFrame(y_scores, columns=['ypred0', 'ypred1'])], axis=1)
        '''x y from id'''
        out['x']=out['id'].astype(str).str.slice(0, 6).astype(int)/10000
        out['y']=out['id'].astype(str).str.slice(6, 12).astype(int)/10000
        '''create quantized levels'''
        # firepercents = [0.01, 0.05, 0.15, 0.5]
        # b=[0, 0.070868188724, 0.270555242059, 0.5920026937689999, 0.871564568438, 1] # see checkscores.ipynb
        # firepercents = [0.005, 0.05, 0.15, 0.5]
        #b=[0, 0.052360608171, 0.270555242059, 0.5920026937689999, 0.871564568438, 1]
        # firepercents = [0.005, 0.05, 0.15, 0.55]
        #b=[0, 0.052360608171, 0.270555242059, 0.5920026937689999, 0.909553812731, 1]
        # firepercents = [0.005, 0.05, 0.20, 0.55]
        b=[0, 0.052360608171, 0.270555242059, 0.67285159829, 0.8910462321779999, 1]
        #old bins [0, 0.2, 0.4, 0.6, 0.8, 1]
        out['risk'] = pd.np.digitize(out['ypred1'], bins=b)
        #out.rename(columns={'xu':'x', 'yu':'y'}, inplace=True)
        out.to_csv(fpred, index=False)
        return fpred
    except:
        print('Failed to create predictions csv %s\n'%fpred+traceback.format_exc())
        return None


def main(args):
    if len(args)!=1:
        nextday = datetime.now()+datetime.timedelta(days=1)
        pdate = nextday.strftime("%Y%m%d")
    else:
        pdate = args[0]
    inputdir='/home/lstam/Documents/data/daily_rasters/'
    modeldir='/home/lstam/Documents/bestmodels'
    inputfile = os.path.join(inputdir,pdate+'.nc')
    if not os.path.isfile(inputfile):
        print('NetCDF input features file not found: %s'%inputfile)
        return
    #inputfile='/home/lstam/Documents/daily_rasters/20230702.nc'
    outdirectory=os.path.join('/home/lstam/Documents/data/daily_rasters/csv',pdate)
    if not os.path.exists(outdirectory): os.makedirs(outdirectory)
    staticfeatures=os.path.join(inputdir,'static/static_aft_15.nc')

    print('Preprocessing and Converting netcdf to csv')
    notnormcsv=netcdf_to_csv(inputfile, staticfeatures, outdirectory, \
                  dropfilters=['curvature','index'], \
                  fillnafilters=[r'corine_(\d+)'], renames={'tp': 'rain_7_days', 'time':'firedate'},
                  calibfilters={'firedate': 'v.str.replace("-","")'}
                  )

    print('Preprocessing and Normalizing')
    #notnormcsv='/home/lstam/Documents/daily_rasters/csv/20230702.csv'
    minmaxfile='norm_values_ref_final.json'

    '''
    Coefficients for fitting weather prediction model params to ERA5 distribution 
    
    Wind Scale
    rex_max: coef.x1: 0.6328444208337474, coef x0. - 0.7653276700164925 
    dom_vel: coef.x1: 0.6264133325122736, coef x0. - 0.09141533012744153

    Rain Scale
    coef.x1: 6.316895637905037, coef  x0: 0
    '''

    normcsv=normalizefile(notnormcsv, minmaxfile, calibfilters={'rain_7_days': '6.31689*v/1000', \
                'max_temp': 'v+273.15', 'min_temp':'v+273.15', 'mean_temp': 'v+273.15',
                'max_dew_temp': 'v+273.15', 'min_dew_temp':'v+273.15', 'mean_dew_temp': 'v+273.15',
                'res_max': '0.63284*v+(-0.76532)', 'dom_vel': '0.62641*v+(-0.09141)'},
                  addcols=['fire'])


    print('Run prediction')
    #normcsv='/home/lstam/Documents/data/daily_rasters/csv/20230713/20230713_norm.csv'
    modelf=os.path.join(modeldir,'entiremodels/hypres_tf_ns_ncv_do_2019_model_id_785_r_0_hybrid2test_1.h5')
                                               #hypres_tf_ns_ncv_do_2019_model_id_785_r_0_hybrid2test_1.h5
    predf = modelrun(normcsv, modelf)

if __name__ == '__main__':
    main(sys.argv[1:])
