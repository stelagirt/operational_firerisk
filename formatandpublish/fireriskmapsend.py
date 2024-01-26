from environment import Environment
from notification import notification
from datetime import datetime, timedelta
import os
import argparse
import traceback
import zipfile
#from AddNewImagesToImageService import publishfile
from AddNewImagesToImageService_v2 import runCommand
from fileutils import find_files
import re

def runpredictions():
    #/ home / sgirtsou / automate_predictions.sh 1
    pass

def getmapfile(env, mapdate):
    datest = mapdate.strftime('%Y%m%d')
    dateform = mapdate.strftime('%d-%m-%Y')
    mapfile = '%s%s'%(datest,env.mapfilesuffix)
    mapsubpath = os.path.join(datest, mapfile)
    mappath = os.path.join(env.mapspath, mapsubpath)
    cellfile = '%s%s' % (datest, env.cellfilesuffix)
    cellsubpath = os.path.join(datest, cellfile)
    cellfilepath = os.path.join(env.mapspath, cellsubpath)
    reclfile = '%s%s' % (datest, env.reclfilesuffix)
    reclfilepath = os.path.join(env.mapspath, datest, reclfile)

    return mappath, dateform, cellfilepath, reclfilepath

def getnearestfile(env, mapdate, paths, suffix):
    lsts=[]
    for p in paths:
        for f in find_files(p, '*%s'%suffix, listtype="list"):
            bname = os.path.basename(f)
            g1 = re.search('^(.*?)%s'%suffix, bname)
            lstdate=datetime.strptime(g1.group(1), "%Y%m%d")
            lsts.append(lstdate)
    nearestdate=max(d for d in lsts if d < mapdate)
    for p in paths:
        nfile=os.path.join(p, nearestdate.strftime('%Y%m%d')+suffix)
        if os.path.exists(nfile):
            nearestfile=nfile
    return nearestfile

def getfeatmaps(env, mapdate):
    lstfile=getnearestfile(env, mapdate, [env.lstpath], '_lst.nc')
    ndvifile = getnearestfile(env, mapdate, [env.ndvi1path, env.ndvi2path], '_vegetation.nc')
    meteofile = os.path.join(env.meteopath,mapdate.strftime('%Y%m%d')+'_meteo.nc')
    return [lstfile, ndvifile, meteofile]

def publishfeatmaps(env, mapdate, featmaps,puburl):
    dateform = mapdate.strftime('%d-%m-%Y')
    now = datetime.now()
    today3pm = now.replace(hour=15, minute=0, second=0, microsecond=0)
    today4pm = now.replace(hour=16, minute=0, second=0, microsecond=0)
    today9pm = now.replace(hour=21, minute=0, second=0, microsecond=0)
    today10pm = now.replace(hour=22, minute=0, second=0, microsecond=0)
    for fm in featmaps:
        try:
            pubf=os.path.join(env.publishedpath,os.path.basename(fm)+'.pub')
            if os.path.exists(fm) and not os.path.exists(pubf):
                runCommand(fm,'add'),
                #publishfile(fm)
                mess = 'Successfull publication of feature file %s to :\n%s'%(fm,puburl)
                notification(env).send_notification('Risk Map Preferred', 'Publish Features', mess)
                with open(pubf, 'w') as f:
                    f.write('published')
            elif not os.path.exists(fm):
                mess = 'Feature file not found : %s' % fm
                if (today3pm <= now <= today4pm) or (today9pm <= now <= today10pm):
                    sendfailurepublish(env, mess, 'Risk Map Preferred')
                else:
                    print(mess)
            elif os.path.exists(pubf):
                mess = 'Feature map %s already published for %s. Stopping publishing' % (fm,dateform)
                # sendfailurepublish(env, mess)
                print(mess)
        except:
            mess = 'Error while publishing : %s' % fm
            sendfailurepublish(env, mess, 'Risk Map Preferred', 'Fail to publish Feature')

def sendmap(env, mapfile, datest):
    mess = 'Xάρτης εκίμησης κινδύνου %s'%datest
    mess += '\n\nΑυτό είναι ένα αυτοματοποιημένο μήνυμα. '
    mess += 'Για πληροφορίες απευθυνθείτε: \n'+\
            'Charalampos Kontoes : Kontoes@noa.gr\n'+\
            'Stella Girtsou : sgirtsou@noa.gr\n'+\
            'Alex Apostolakis: alex.apostolakis@noa.gr'
    mess += '\n\nΗ εκτίμηση κινδύνου που εμφανίζεται στον χάρτη είναι υπό αξιολόγηση στα πλαίσια ερευνητικού έργου.'
    #print(mess)
    notification(env).send_notification('Risk Map', 'Send', mess, titlesuf=datest, attachments=[mapfile])

def reclsendmap(env, files, datest):
    mess = 'Xάρτης εκίμησης κινδύνου πυρκαγιάς Νομών %s'%datest
    notification(env).send_notification('Risk Map', 'Nomoi', mess, titlesuf=datest, attachments=files)

def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files + dirs:
            ziph.write(os.path.join(root, file),\
                       os.path.relpath(os.path.join(root, file), os.path.join(path, os.path.pardir)),\
                       compress_type=zipfile.ZIP_DEFLATED)

def zipfiles(filelist, ziph):
    # ziph is zipfile handle
    for f in filelist:
        ziph.write(f, os.path.relpath(f, os.path.join(os.path.dirname(f), os.path.pardir)),\
                   compress_type=zipfile.ZIP_DEFLATED)

def zipmap(mapfile):
    zipmapfile = mapfile+'.zip'
    zipf = zipfile.ZipFile(zipmapfile, mode='w', allowZip64=True)
    zipfiles([mapfile], zipf)
    zipf.close()
    return zipmapfile

def sendfailurenotice(env, mess):
    print(mess)
    notification(env).send_notification('Risk Map', 'Fail to send', mess)

def sendfailurepublish(env, mess, service, action='Fail to publish'):
    print(mess)
    notification(env).send_notification(service, action, mess)

def sendfailurenomoi(env, mess):
    print(mess)
    notification(env).send_notification('Risk Map', 'Nomoi Fail', mess)

def getmapdate(argdate, argdays):
    if argdate is None and argdays is None:
        mapdate = datetime.today() + timedelta(days=1)
    elif argdate is not None:
        mapdate = argdate
    elif argdays is not None:
        mapdate = datetime.today() - timedelta(days=argdays) + timedelta(days=2)
    return mapdate


def sendriskmap(args):
    mapdate=args['map_date']
    argdays=args['past_days']
    compress=args['no_compress']
    riskmapsend=args['risk_map']
    risknomoisend = args['risk_nomoi']
    publishmap = args['publish_map']
    featuremaps = args['publish_features']

    env = Environment()

    now = datetime.now()
    now = now.replace(microsecond=0)
    today3pm = now.replace(hour=15, minute=0, second=0, microsecond=0)
    today4pm = now.replace(hour=16, minute=0, second=0, microsecond=0)
    today9pm = now.replace(hour=21, minute=0, second=0, microsecond=0)
    today10pm = now.replace(hour=22, minute=0, second=0, microsecond=0)

    print('%s Starting publishing process' % now)
    dateform = None
    mapfile, dateform, cellfile, reclfile = getmapfile(env, getmapdate(mapdate, argdays))
    featmaps = getfeatmaps(env, mapdate)

    #print('Searching for map file path : %s'%mapfile)
    #runpredictions()
    '''Publish to fire risk web app'''
    pubf = os.path.join(os.path.dirname(cellfile), 'published.txt')
    nomoif = os.path.join(os.path.dirname(reclfile), 'sendnomoi.txt')
    puburl='https://arcgis.geoapikonisis.gr/preferred/'
    if publishmap:
        try:
            if os.path.exists(cellfile) and not os.path.exists(pubf):
                #cellToPNG.publish(cellfile)
                runCommand(cellfile, runmode='add')
                #publishfile(cellfile)
                mess = 'Successfull publication to :\n%s'%puburl
                notification(env).send_notification('Risk Map Preferred', 'Publish', mess, titlesuf=dateform)
                with open(pubf, 'w') as f:
                    f.write('published')
            elif not os.path.exists(cellfile):
                mess = 'Cell file not found : %s' % cellfile
                print(mess)
                if (today3pm < now < today4pm) or (today9pm < now < today10pm):
                    sendfailurepublish(env, mess, 'Risk Map Preferred')
            elif os.path.exists(pubf):
                mess = '%s already published for %s. Stopping publishing' % (cellfile,dateform)
                #sendfailurepublish(env, mess)
                print(mess)
        except:
            mess = 'Error while publishing : %s' % cellfile
            print(traceback.format_exc())
            if (today3pm <= now <= today4pm) or (today9pm <= now <= today10pm):
                sendfailurepublish(env, mess, 'Risk Map Preferred')
    '''feature maps'''
    if featuremaps:
        publishfeatmaps(env, mapdate, featmaps, puburl)

    '''Send fire risk web app'''
    if riskmapsend:
        try:
            if os.path.exists(mapfile):
                if compress:
                    print('Compressing map : %s' % mapfile)
                    mapfile = zipmap(mapfile)
                print('Sending map : %s' % mapfile)
                sendmap(env, mapfile, dateform)
            else:
                mess = 'Map not found : %s' % mapfile
                sendfailurenotice(env, mess)
        except:
            mess = 'Error sending map for %s: \n'%dateform+traceback.format_exc()
            sendfailurenotice(env, mess)
    '''Send nomoi risk '''
    if risknomoisend:
        try:
            ext='.png'
            nomoifiles = ["nomoirisk.csv", 'freqrisk'+ext, 'meanrisk'+ext]
            basepath = os.path.dirname(reclfile)
            if os.path.exists(reclfile) and not os.path.exists(nomoif):
                nomoifiles = [os.path.join(basepath,rf) for rf in nomoifiles]
                datest = dateform[6:10] + dateform[3:5] + dateform[0:2]
                #nomoi_risk.NomoiRisk(datest)
                if all(os.path.exists(rf) for rf in nomoifiles):
                    reclsendmap(env, nomoifiles, dateform)
                    with open(nomoif, 'w') as f:
                        f.write('Nomoi sent')
                else:
                    mess = 'Some files are Missing : %s' % nomoifiles
                    sendfailurenomoi(env, mess)
            elif not os.path.exists(reclfile):
                mess = 'Reclassified file not found : %s' % reclfile
                if (today3pm < now < today4pm) or (today9pm < now < today10pm):
                    sendfailurenomoi(env, mess)
                else:
                    print(mess)
            elif os.path.exists(nomoif):
                mess = 'Already sent Nomoi for %s.' % dateform
                print(mess)
        except:
            mess = 'Error sending nomoi risk for %s: \n'%dateform+traceback.format_exc()
            sendfailurenomoi(env, mess)

def getargs():
    parser = argparse.ArgumentParser(description='Fire Risk Map sending')
    parser.add_argument('-d','--map-date', type=lambda s: datetime.strptime(s, '%Y%m%d'),
                        help='The date of the Fire risk Map in format : DD-MM-YYYY. Default is tomorrow '
                                         'date')
    parser.add_argument('-nc', '--no-compress', action='store_false',
                        help='Compress map file')
    parser.add_argument('-p', '--past-days', type=int, help='days before current date')
    parser.add_argument('-rsm', '--risk-map', action='store_true',
                        help='Send map file')
    parser.add_argument('-pm', '--publish-map', action='store_true',
                        help='Publish map file')
    parser.add_argument('-rn', '--risk-nomoi', action='store_true',
                        help='Send Nomoi Risk')
    parser.add_argument('-pfm', '--publish-features', action='store_true',
                        help='Publish Feature Maps')
    args = parser.parse_args()
    return vars(args)

def main():
    args = getargs()
    sendriskmap(args)

if __name__ == "__main__":
    main()


