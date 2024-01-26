import pandas as pd
import os
from sklearn.model_selection import train_test_split
import normdataset


def convertonehot(X, col, r, del0=False):
    Xbin = pd.get_dummies(X[col].round())
    if del0 and (0 in Xbin.columns):
        del Xbin[0]
    dmaxcols = []
    for i in r:
        dmaxcols.append('bin_%s_%d' %(col, i))
    diffcol=set(r) - set([int(c) for c in Xbin.columns])
    if len(diffcol)>0:
        for c in diffcol:
            Xbin[c]=0
        Xbin=Xbin.reindex(sorted(Xbin.columns), axis=1)
    if len(Xbin.columns) != len(dmaxcols):
        Xbin = Xbin.iloc[:,0:len(dmaxcols)]
        print('WARNING! different columns for onehot "%s"'%col)
    Xbin.columns = dmaxcols
    del X[col]
    X = pd.concat([X, Xbin], axis=1)
    return X


def prepare_dataset(df, X_columns, y_columns, firedate_col, ohecols,calib=None, returnid=False):
                    #corine_col, domdir_col, dirmax_col, week_col, month_col, calib=None):
    if returnid and 'id' in df:
        df = df[X_columns+y_columns+[firedate_col] + ['id']]
    else:
        df = df[X_columns + y_columns + [firedate_col]]
    print('before nan drop: %d' % len(df.index))
    df = df.dropna()
    print('after nan drop: %d' % len(df.index))
    df = df.drop_duplicates(keep='first')
    df.reset_index(inplace=True, drop=True)
    print('after dup. drop: %d' % len(df.index))
    print('renaming "x": "xpos", "y": "ypos"')
    X_tr, y_int = df[X_columns], df[y_columns]
    X_tr = X_tr.rename(columns={'x': 'xpos', 'y': 'ypos'})

    #calibration
    if calib is not None:
        for k in calib:
            print("calibrating %s by %.1f"%(k,calib[k]))
            X_tr[k] = X_tr[k]*(1+calib[k])

    #convert onehot
    for ohecol in ohecols:
        if ohecol is not None:
            if not ohecols[ohecol]['range']:
                rmin=int(df[ohecol].unique().min())
                rmax=int(df[ohecol].unique().max())+1
                ohecol['range']=range(rmin, rmax)
            X_tr = convertonehot(X_tr, ohecol,ohecols[ohecol]['range'], ohecols[ohecol]['del0'])

    X = X_tr
    y = y_int
    groupspd = df[firedate_col]
    if returnid:
        if "id" in df.columns:
            idpd = df["id"]
        else:
            idpd = df.index.to_series()
        df = None
        return X, y, groupspd, idpd
    else:
        df = None
        return X, y, groupspd, None

def check_categorical(df, checkcol, newcols):
    cat_cols = [c for c in df.columns if checkcol.upper() in c.upper()]
    if any([c.upper() == checkcol.upper() for c in cat_cols]) and len(cat_cols) > 1:
        cat_col = [c for c in df.columns if checkcol.upper() == c.upper()][0]
        deletecolumns = []
        for c in newcols:
            if (c.upper() != checkcol.upper() and checkcol.upper() in c.upper()):
                deletecolumns.append(c)
        for c in deletecolumns:
            newcols.remove(c)
    elif any([c.upper() == checkcol.upper() for c in cat_cols]) and len(cat_cols) == 1:
        cat_col = [c for c in df.columns if checkcol.upper() == c.upper()][0]
    elif not any([c.upper() == checkcol.upper() for c in cat_cols]) and len(cat_cols) == 1:
        cat_col = [c for c in df.columns if checkcol.upper() == c.upper()][0]
    elif not any([c.upper() == checkcol.upper() for c in cat_cols]) and len(cat_cols) > 1:
        cat_col = None
        for i in range(0, len(newcols)):
            c = newcols[i]
            if c.upper() != checkcol.upper() and checkcol.upper() in c.upper() and not c.startswith('bin_'):
                newname = "bin_" + c
                newcols[i] = newname
                df.rename(columns={c: newname}, inplace=True)
    else:
        cat_col = None
    return cat_col, newcols

def create_ds_parts(dsfile, class0nrows, dffirefile, dfpartfile, debug = False):
    df = pd.read_csv(dsfile)
    if debug:
        print("Split dataset to fire no-fire")
    firegroup = df.groupby('fire')
    dfclass0 = firegroup.get_group(0)
    dffire = None
    if 1 in firegroup.groups:
        if debug:
            print("Creating fire dataset %s" % dffirefile)
        dffire = firegroup.get_group(1)
        dffire.to_csv(dffirefile, index=False)
    else:
        df[0:0].to_csv(dffirefile, header=True, index=False)
    tsize = class0nrows / len(dfclass0.index)
    if debug:
        print("Stratify shuffling dataset %s" % dffirefile)
    X_train, dfpart, y_train, y_test = train_test_split(dfclass0, dfclass0['firedate'], test_size=tsize, stratify=dfclass0['firedate'], shuffle=True)
    if debug:
        print("Creating no fire dataset %s" % dfpartfile)
    #dfpart = pd.concat([X_test,y_test],axis=1)
    dfpart.to_csv(dfpartfile, index=False)
    if dffire is not None:
        df = pd.concat([dfpart, dffire])
    else:
        df = dfpart
    return df

# load the dataset
def load_dataset(trfiles, featuredrop=[], class0nrows=0, debug=True, returnid=False, calib=None):
    #categorical columns
    domdircheck = 'dom_dir'
    dirmaxcheck = 'dir_max'
    corinecheck = 'Corine'
    monthcheck = 'month'
    wkdcheck = 'weekday'
    catcols={domdircheck:{'del0':True, 'range': range(1,9)}, dirmaxcheck: {'del0':True, 'range': range(1,9)},
               monthcheck: {'del0': False, 'range': range(3, 11)}, wkdcheck: {'del0': False, 'range': range(1, 8)},
               corinecheck: {'del0':False, 'range': None}}

    #group variable column for cross validation
    firedatecheck = 'firedate'

    #independent variables columns
    X_columns = {'include':['max_temp', 'min_temp', 'mean_temp', 'res_max', dirmaxcheck, 'dom_vel', domdircheck,
                 'rain_7days', corinecheck, 'Slope', 'DEM', 'Curvature', 'Aspect', 'ndvi', 'evi', 'lst_day',
                 'lst_night', monthcheck, wkdcheck,
                 'mean_dew_temp', 'max_dew_temp', 'min_dew_temp','frequency', 'f81', 'x', 'y', 'pop', 'road_dens'],
                 'exclude':['index']}
    
    #dependent variable column
    y_columns = ['fire']

    #loading training set
    if isinstance(trfiles, list):
        if debug:
            print("Loading full dataset ...")
        dflist=[]
        for dsfile in trfiles:
            if debug:
                print("Loading dataset file %s" % dsfile)
            dflist.append(pd.read_csv(dsfile))
        df = pd.concat(dflist)
    else:
        dsfile = trfiles
        if class0nrows > 0:
            print("Loading shuffled, stratified %d rows of dataset %s" % (class0nrows,dsfile))
            dffirefile = dsfile[0:-4]+"_fires.csv"
            dfpartfile = dsfile[0:-4] + "_part.csv"
            if os.path.isfile(dffirefile) and os.path.isfile(dfpartfile):
                if debug:
                    print("Loading fire dataset %s"%dffirefile)
                dffire = pd.read_csv(dffirefile)
                if debug:
                    print("Loading no-fire dataset %s"%dfpartfile)
                #dfpart = pd.read_csv(dsfile, nrows=class0nrows)
                #dfpart = dfpart[dfpart['fire']!=1]
                dfpart = pd.read_csv(dfpartfile)
                if len(dffire.index) > 0:
                    df = pd.concat([dfpart, dffire])
                else:
                    if debug:
                        print("Fire dataset is empty")
                    df = dfpart
            else:
                df = create_ds_parts(dsfile, class0nrows, dffirefile, dfpartfile, debug)
        else:
            if debug:
                print("Loading full dataset %s" % dsfile)
            df = pd.read_csv(dsfile)

    #process loaded dataset

    #filter columns
    X_columns_upper = [c.upper() for c in X_columns['include']]
    X_exclude=[c.upper() for c in X_columns['exclude']]
    newcols = [c for c in df.columns if
               c.upper() in X_columns_upper or any([cX in c.upper() for cX in X_columns_upper])]
    excludecols = [c for c in df.columns if
               c.upper() in X_exclude or any([cX in c.upper() for cX in X_exclude])]
    newcols = list(set(newcols)-set(excludecols))
    X_columns = newcols

    #check categorical variables for existing one hot columns
    ohecols={}
    for catcol in catcols:
        ohecol, newcols = check_categorical(df, catcol, newcols)
        ohecols[ohecol]=catcols[catcol]

    #find group variable for CV
    firedate_col = [c for c in df.columns if firedatecheck.upper() in c.upper()][0]

    #convert categorical to onehot and calibrate values if needed
    X, y, groupspd, idpd = prepare_dataset(df, X_columns, y_columns, firedate_col, ohecols, calib, returnid)

    #columns ignored from loade dataset after processing
    print("Ignored columns from csv %s"%([c for c in df.columns if c not in X.columns]))

    X_columns = X.columns

    #drop feature columns defined by hyperparamaters
    if len(featuredrop) > 0:
        X = X.drop(columns=[c for c in X.columns if any([fd in c for fd in featuredrop])])
    print("Dropped columns %s"%(list(set(X_columns)-set(X.columns))))
    #if debug:
    #    print("X helth check %s"%X.describe())
    #    print("y helth check %s"%y.describe())
    if returnid:
        return X, y, groupspd, idpd
    else:
        return X, y, groupspd

