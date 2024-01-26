import pandas as pd
import fileutils

def retrieve_best_models(dir, filepattern, metrics, valst, testst, filters = [], modelcount=1):
    best_models={}
    setfiles = [f for f in fileutils.find_files(dir, filepattern, listtype="walk")]
    df = None
    for f in setfiles:
        dftemp=pd.read_csv(f)
        df = dftemp if df is None else pd.concat([df,dftemp])
    for metric in metrics:
        df_flt = df
        for filt in filters:
            df_flt = df_flt[eval(filt)]
        df_sorted = df_flt.sort_values(by=['%s %s'%(metric,valst)], ascending=False)
        #best_models['%s %s'%(metric,testst)] = [{'params':eval(df_sorted.iloc[0]['params']), 'trial':df_sorted.iloc[0]['trial']}]
        best_models['%s %s' % (metric, testst)]=[]
        for i in range(modelcount):
            if 'trial' in df_sorted.columns:
                best_models['%s %s'%(metric,testst)] += [{'params':eval(df_sorted.iloc[i]['params']), 'trial':df_sorted.iloc[i]['trial']}]
            else:
                best_models['%s %s'%(metric,testst)] += [{'params':eval(df_sorted.iloc[i]['params']), 'trial':i}]
    return best_models
'''
metrics=['auc', 'hybrid2', 'hybrid5', 'NH2', 'NH5', 'NH10']
best_models=retrieve_best_models('/home/aapostolakis/Documents/ffpdata/results/aris/', '*2018only*', metrics, 'val.', 'test')
for m in metrics:
    print('%s test : %s'%(m,best_models['%s test'%(m)]))
'''
def retrieve_models_by_id(modelid, metric, dir, filepattern, metrics, valst, testst, filters = [], modelcount=1):
    bm = retrieve_best_models(dir, filepattern, metrics, valst, testst, filters, modelcount)
    cnt=0
    for m in bm[metric]:
        if m['trial']==modelid:
            cnt+=1
            rm=m
    if cnt==1:
        return rm
    else:
        return None