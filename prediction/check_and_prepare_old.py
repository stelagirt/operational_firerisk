import pandas as pd
import os
from sklearn.model_selection import train_test_split

def prepare_dataset(df, X_columns, y_columns, firedate_col, corine_col, domdir_col, dirmax_col, calib=None):
    df = df[X_columns+y_columns+[firedate_col]]
    print('before nan drop: %d' % len(df.index))
    df = df.dropna()
    print('after nan drop: %d' % len(df.index))
    df = df.drop_duplicates(keep='first')
    df.reset_index(inplace=True, drop=True)
    print('after dup. drop: %d' % len(df.index))
    print('renaming "x": "xpos", "y": "ypos"')
    X_unnorm, y_int = df[X_columns], df[y_columns]
    X_unnorm = X_unnorm.rename(columns={'x': 'xpos', 'y': 'ypos'})

    #calibration
    if calib is not None:
        for k in calib:
            print("calibrating %s by %.1f"%(k,calib[k]))
            X_unnorm[k] = X_unnorm[k]*(1+calib[k])

    # categories to binary
    if domdir_col:
        Xbindomdir = pd.get_dummies(X_unnorm[domdir_col].round())
        if 0 in Xbindomdir.columns:
            del Xbindomdir[0]
        ddircols = []
        for i in range(1, 9):
            ddircols.append('bin_dom_dir_%d' % i)
        Xbindomdir.columns = ddircols
        del X_unnorm[domdir_col]
        X_unnorm = pd.concat([X_unnorm, Xbindomdir], axis=1)

    if dirmax_col:
        Xbindirmax = pd.get_dummies(X_unnorm[dirmax_col].round())
        if 0 in Xbindirmax.columns:
            del Xbindirmax[0]
        dmaxcols = []
        for i in range(1, 9):
            dmaxcols.append('bin_dir_max_%d' % i)
        Xbindirmax.columns = dmaxcols
        del X_unnorm[dirmax_col]
        X_unnorm = pd.concat([X_unnorm, Xbindirmax], axis=1)

    if corine_col:
        # convert corine level
        corine2 = X_unnorm[corine_col].copy() // 10
        del X_unnorm[corine_col]
        # X_unnorm.rename(columns={corine_col: 'corine_orig'})
        X_unnorm = pd.concat([X_unnorm, corine2], axis=1)

        Xbincorine = pd.get_dummies(X_unnorm[corine_col])
        corcols = ['bin_corine_' + str(c) for c in Xbincorine.columns]
        Xbincorine.columns = corcols
        del X_unnorm[corine_col]
        X_unnorm = pd.concat([X_unnorm, Xbincorine], axis=1)

    # X = normdataset.normalize_dataset(X_unnorm, aggrfile='stats/featurestats.json')
    X = X_unnorm
    y = y_int
    groupspd = df[firedate_col]
    return X, y, groupspd

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
    # dsfile = 'dataset_ndvi_lu.csv'
    domdircheck = 'dom_dir'
    dirmaxcheck = 'dir_max'
    corinecheck = 'Corine'
    monthcheck = 'month'
    wkdcheck = 'wkd'
    firedatecheck = 'firedate'
    X_columns = ['max_temp', 'min_temp', 'mean_temp', 'res_max', dirmaxcheck, 'dom_vel', domdircheck,
                 'rain_7days', corinecheck, 'Slope', 'DEM', 'Curvature', 'Aspect', 'ndvi', 'evi', 'lst_day',
                 'lst_night', monthcheck, wkdcheck,
                 'mean_dew_temp', 'max_dew_temp', 'min_dew_temp','frequency', 'f81', 'x', 'y']
    y_columns = ['fire']
    # if not os.path.exists(os.path.join(dsetfolder, dsready)):
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

    X_columns_upper = [c.upper() for c in X_columns]
    newcols = [c for c in df.columns if
               c.upper() in X_columns_upper or any([cX in c.upper() for cX in X_columns_upper])]
    X_columns = newcols
    corine_col, newcols = check_categorical(df, corinecheck, newcols)
    dirmax_col, newcols = check_categorical(df, dirmaxcheck, newcols)
    domdir_col, newcols = check_categorical(df, domdircheck, newcols)
    month_col, newcols = check_categorical(df, monthcheck, newcols)
    wkd_col, newcols = check_categorical(df, wkdcheck, newcols)

    firedate_col = [c for c in df.columns if firedatecheck.upper() in c.upper()][0]
    X, y, groupspd = prepare_dataset(df, X_columns, y_columns, firedate_col, corine_col, domdir_col, dirmax_col, calib)
    print("Ignored columns from csv %s"%([c for c in df.columns if c not in X.columns]))
    idpd = df['id']
    df = None
    X_columns = X.columns
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
