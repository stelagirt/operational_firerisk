import pandas as pd
import geopandas as gpd
import multiprocessing as mp
import time
import os
import fileutils

'''
periphereies
0 	Π. ΑΝΑΤΟΛΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ - ΘΡΑΚΗΣ 	
1 	Π. ΚΕΝΤΡΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ 	
2 	Π. ΔΥΤΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ 	
3 	Π. ΗΠΕΙΡΟΥ 	MULTIPOLYGON 
4 	Π. ΘΕΣΣΑΛΙΑΣ 	
5 	Π. ΒΟΡΕΙΟΥ ΑΙΓΑΙΟΥ 	
6 	Π. ΝΟΤΙΟΥ ΑΙΓΑΙΟΥ 	
7 	Π. ΣΤΕΡΕΑΣ ΕΛΛΑΔΑΣ 	
8 	Π. ΔΥΤΙΚΗΣ ΕΛΛΑΔΑΣ 	
9 	Π. ΠΕΛΟΠΟΝΝΗΣΟΥ 	
10 	Π. ΙΟΝΙΩΝ ΝΗΣΩΝ 	
11 	Π. ΚΡΗΤΗΣ 	
12 	Π. ΑΤΤΙΚΗΣ 	
'''
'''
walk folders to find all dataset files
'''
def walkmonthdays(sfolder, pattern, walktype='walk'):
    dayfiles = []
    for dayf in fileutils.find_files(sfolder, pattern, listtype="walk"):
        dayfiles += [dayf]
    return dayfiles

def new_process(func, proclist, args):
    q = mp.Queue()
    proclist += [{'proc': mp.Process(target=func, args=args), 'queue': q}]
    proclist[-1]['proc'].start()

def par_files(func, days, pthreads, args):
    procs = []
    proctimetotal = 0
    dayscompleted = []
    #print(days)
    for cpu in range(pthreads):
        d = days.pop()
        dayscompleted += [d]
        #print('initial proc')
        new_process(func, procs, tuple([d]+args))
    while len(procs) > 0:
        time.sleep(0.1)
        for p in procs:
            try:
                proctimetotal += p['queue'].get_nowait()
            except:
                pass
            if not p['proc'].is_alive():
                #print('remove, tot procs: %d' % len(procs))
                procs.remove(p)
                #print('tot procs: %d' % len(procs))
        while len(procs) < pthreads:
            if len(days) == 0: break
            #print('new proc')
            d = days.pop()
            dayscompleted += [d]
            new_process(func, procs, tuple([d]+args))
    return proctimetotal

def cropfile(f, cropfolder, gdfperif, suffix):
    fcrop = os.path.join(cropfolder, os.path.basename(f).split('.')[0] + suffix + '.csv')
    if os.path.isfile(fcrop):
        return fcrop
    print('Start processing file %s'%f)
    df=pd.read_csv(f)
    geom = gpd.points_from_xy(df['x'], df['y'])
    gdf = gpd.GeoDataFrame(df, geometry=geom)
    gdf = gdf.set_crs(4326)
    gdf_crop = gpd.sjoin(gdf, gdfperif, how='inner', op='within')
    df_crop = pd.DataFrame(gdf_crop.drop(columns=['geometry','PER','index_right']))
    df_crop.to_csv(fcrop,index=False)
    print('Done processing file. Output %s' % fcrop)
    return fcrop

'''
Examples:

start=time.time()
print("Start Cropping")
cropfolder='/mnt/nvme2tb/ffp/datasets/test/2019/greece'
if not os.path.isdir(cropfolder): os.makedirs(cropfolder)
gdfperif=gpd.read_file(r'/mnt/nvme2tb/ffp/datasets/test/2019/perif/periphereies.shp',encoding='Windows-1253')
gdfperif=gdfperif.to_crs(4326)

id_perif=7
#cropfile('/mnt/nvme2tb/ffp/datasets/traindataset_new.csv', cropfolder, gdfperif.iloc[[id_perif]], '_sterea')
#cropfile('/mnt/nvme2tb/ffp/datasets/train/train_new_sample_1_2_fna.csv', cropfolder, gdfperif.iloc[[id_perif]], '_sterea')
#cropfile('/mnt/nvme2tb/ffp/datasets/test/2019/20190930_df.csv', cropfolder, gdfperif.iloc[[id_perif]], '_sterea')
cropfile('/mnt/nvme2tb/ffp/datasets/train/train_new_sample_1_2.csv', cropfolder, gdfperif.iloc[[id_perif]], '_sterea')

id_perif=12
#cropfile('/mnt/nvme2tb/ffp/datasets/train/train_new_sample_1_2_fna.csv', cropfolder, gdfperif.iloc[[id_perif]], '_attica')
cropfile('/mnt/nvme2tb/ffp/datasets/train/train_new_sample_1_2.csv', cropfolder, gdfperif.iloc[[id_perif]], '_attica')
'''
'''
dayfiles=walkmonthdays('/mnt/nvme2tb/ffp/datasets/test/2019', '*_df.csv','list')
proctime=par_files(cropfile, dayfiles, mp.cpu_count()-2, [cropfolder, gdfperif, '_greece'])
dur=time.time()-start
print("Done in %d min and %d secs"%(int(dur/60), dur%60))

cropfile('/mnt/nvme2tb/ffp/datasets/train/train_new_sample_1_2.csv', '/mnt/nvme2tb/ffp/datasets/train/', gdfperif, '_greece')
'''
