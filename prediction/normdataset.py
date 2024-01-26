from pandas import DataFrame
import os
import json
import traceback
def normalized_values(y,dfmax, dfmin, dfmean, dfstd, t = None):
    if not t:
        a = (y- dfmin) / (dfmax - dfmin)
        return(a)
    elif t=='std':
        a = (y - dfmean) / dfstd
        return(a)
    elif t=='no':
        return y

def dataset_sanity_check(df):
    for c in [cl for cl in df.columns if 'bin' not in cl]:
        print('column %s - max: %s, min : %s, mean: %s, std: %s'%(c, df[c].max(), df[c].min(), df[c].mean(), df[c].std()))

def apply_norm(normdf, unnormdf, col, dfmax, dfmin, dfmean, dfstd, norm_type):
    normdf[col] = unnormdf.apply(lambda x: normalized_values(x[col], dfmax, dfmin, dfmean, dfstd, norm_type), axis=1)

def normalize_dataset(df, norm_type = None, aggrfile = None, check=True):
    X = DataFrame()
    aggrs = None
    if aggrfile and os.path.exists(aggrfile):
        with open(aggrfile) as aggrf:
            aggrs = json.loads(aggrf.read())
    else:
        if check: print("No aggregation file")
    for c in df.columns:
        try:
            dfcfloat = df[c].astype('float32')
            if not 'bin' in c:
                if check: print("Normalize column:%s" % c)
                if not aggrs is None:
                    if not c in aggrs:
                        if check: print("Failed to find aggregations for %s" % c)
                        X[c] = df[c]
                        continue
                    dfmax = aggrs[c]['max'] if 'max' in aggrs[c] else None
                    dfmin = aggrs[c]['min'] if 'min' in aggrs[c] else None
                    dfmean = aggrs[c]['mean'] if 'mean' in aggrs[c] else None
                    dfstd = aggrs[c]['std'] if 'std' in aggrs[c] else None
                else:
                    dfmax = dfcfloat.max()
                    dfmin = dfcfloat.min()
                    dfmean = dfcfloat.mean()
                    dfstd = dfcfloat.std()
                X[c] = dfcfloat.apply(lambda x: normalized_values(x, dfmax, dfmin, dfmean, dfstd, norm_type))#, axis=1)
                if check:
                    dataset_sanity_check(X[[c]])
            else:
                X[c] = df[c]
        except:
            print('error while processing column %s'%c+"\n"+traceback.format_exc())

    return X

def convtoindex(y, lu_dict):
    return(lu_dict[y])

def indexdict(dfcol):
    lu = list(dfcol.unique())
    lu_dict = {x:lu.index(x)+1 for x in lu}
    return lu_dict

def index_string_values(X_unnorm, str_classes):
    indexdicts = {}
    for str_class in str_classes:
        indexdicts[str_class]=indexdict(X_unnorm[str_class])
    X_unnorm_int = X_unnorm.copy()
    for c in str_classes:
        print(c)
        X_unnorm_int[c] = X_unnorm.apply(lambda x: convtoindex(x[c],indexdicts[c]),axis=1)
    return X_unnorm_int
