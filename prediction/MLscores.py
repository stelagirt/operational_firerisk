from sklearn.metrics import confusion_matrix, accuracy_score, recall_score, f1_score, precision_score
from keras.metrics import AUC
import keras.backend as K
import re

def recall(tp,fn):
    #if tp+fn == 0:
    #    return -1000
    return tp/(tp+fn+K.epsilon())

def precision(tp,fp):
    if tp+fp == 0:
        return -1000
    return tp/(tp+fp)

def accuracy(tp,tn,fp,fn):
    if tp+tn+fp+fn == 0:
        return -1000
    return (tp+tn)/(tp+tn+fp+fn)

def balanced_accuracy(tp,tn,fp,fn):
    if tp+tn+fp+fn == 0:
        return -1000
    return (tp+tn)/(tp+tn+fp+fn)

def f1(tp,fp,fn):
    if recall(tp,fn)+precision(tp,fp) == 0:
        return -1000
    return 2*recall(tp,fn)*precision(tp,fp)/(recall(tp,fn)+precision(tp,fp))

def npv(tn,fn):
    if tn+fn == 0:
        return -1000
    return tn/(tn+fn)

def hybridrecall(w1, w0, rec1, rec0, hybtype = 'hybrid'):
    if hybtype == 'hybrid':
        if rec1 > 0 and rec0 > 0:
            return (w1 + w0) / (w1 / rec1 + w0 / rec0)
        else:
            return -1000
    elif hybtype == 'NH':
        return w1*rec1 + w0*rec0
    else:
        return -1000


def calc_model_distrib(y_scores, y, thress, cnttype='FN'):
    if cnttype=='FN':
        n = y[(y_scores < thress) & (y == 1)].shape[0]
    elif cnttype=='TP':
        n = y[(y_scores >= thress) & (y == 1)].shape[0]
    elif cnttype=='TN':
        n = y[(y_scores < thress) & (y == 0)].shape[0]
    elif cnttype=='FP':
        n = y[(y_scores >= thress) & (y == 0)].shape[0]
    return n

def calc_all_model_distrib(y_scores, y, debug=True):
    fn01 = calc_model_distrib(y_scores, y, 0.1, 'FN')
    fn02 = calc_model_distrib(y_scores, y, 0.2, 'FN')
    fn001 = calc_model_distrib(y_scores, y, 0.01, 'FN')
    fn002 = calc_model_distrib(y_scores, y, 0.02, 'FN')
    if debug:
        print('FN01: %d'%fn01)
        print('FN02: %d'%fn02)
        print('FN001: %d'%fn001)
        print('FN002: %d'%fn002)
    return fn01, fn02, fn001, fn002

def calc_all_hybrids(rec_1, rec_0, debug=True):
    hybrid1 = hybridrecall(1, 1, rec_1, rec_0)
    hybrid2 = hybridrecall(2, 1, rec_1, rec_0)
    hybrid5 = hybridrecall(5, 1, rec_1, rec_0)
    nh2 = hybridrecall(2, 1, rec_1, rec_0, 'NH')
    nh5 = hybridrecall(5, 1, rec_1, rec_0, 'NH')
    nh10 = hybridrecall(10, 1, rec_1, rec_0, 'NH')
    ba = hybridrecall(1, 1, rec_1, rec_0, 'NH')

    if debug:
        print("hybrid1 : %.2f" % hybrid1)
        print("hybrid2 : %.2f" % hybrid2)
        print("hybrid5 : %.2f" % hybrid5)
        print("NH 2 : %.2f" % nh2)
        print("NH 5 : %.2f" % nh5)
        print("NH 10 : %.2f" % nh10)
    return hybrid1, hybrid2, hybrid5, nh2, nh5, nh10, ba

def cmvals(y_true, y_pred):
    cm = confusion_matrix(y_true, y_pred)
    if cm.shape[0]==1 and y_true[0]==0:
        tn = cm[0,0]
        fp = 0
        fn = 0
        tp = 0
    elif cm.shape[0]==1 and y_true[0]==1:
        tn = 0
        fp = 0
        fn = 0
        tp = cm[0,0]
    elif cm.shape[0]==2:
        tn = cm[0, 0]
        fp = cm[0, 1]
        fn = cm[1, 0]
        tp = cm[1, 1]
    else:
        tn=fp=fn=tp=None
    return tn, fp, fn, tp

def cmvals_tf(y, y_pred):
    tp = K.sum(y_pred * y)
    fn = K.sum((1 - y_pred) * y)
    tn = K.sum((1 - y_pred) * (1 - y))
    fp = K.sum(y_pred * (1 - y))
    return tn, fp, fn, tp

def calc_metrics(y, y_scores, y_pred, numaucthres=200, debug=True, calc_hybrids = False):
    if debug:
        print("calulating metrics from scores (sklearn)")
        print("calulating tn, fp, fn, tp")
    tn, fp, fn, tp = cmvals(y, y_pred)
    if debug:
        print("tn : %d, fp : %d, fn : %d, tp : %d" % (tn, fp, fn, tp))
    tnK, fpK, fnK, tpK = cmvals_tf(y, y_pred)
    if debug:
        print("ksum tn : %d, fp : %d, fn : %d, tp : %d" % (tnK, fpK, fnK, tpK))
    if debug:
        print("calulating auc...")
    if numaucthres>0:
        aucmetric = AUC(num_thresholds=numaucthres)
        aucmetric.update_state(y, y_scores[:, 1])
        auc = float(aucmetric.result())
    else:
        auc = 0.0
    if debug:
        print("auc : %.2f" % auc)
    if debug:
        print("calulating accuracy...")
    acc = accuracy_score(y, y_pred)
    #acc_0 = accuracy_score(1 - y, 1 - y_pred)
    if debug:
        print("accuracy : %.2f" % acc)
        #print("accuracy 0 : %.2f" % acc_0)
    if debug:
        print("calulating recall...")
    rec_1 = recall_score(y, y_pred)
    rec_0 = recall_score(1 - y, 1 - y_pred)
    if debug:
        print("recall 1 : %.2f" % rec_1)
        print("recall 0 : %.2f" % rec_0)
    if debug:
        print("calulating precision...")
    prec_1 = precision_score(y, y_pred)
    prec_0 = precision_score(1 - y, 1 - y_pred)
    if debug:
        print("precision 1 : %.2f" % prec_1)
        print("precision 0 : %.2f" % prec_0)
    if debug:
        print("calulating f1 score...")
    f1_1 = f1_score(y, y_pred)
    f1_0 = f1_score(1 - y, 1 - y_pred)
    if debug:
        print("f1 1 : %.2f" % f1_1)
        print("f1 0 : %.2f" % f1_0)
    if calc_hybrids:
        if debug:
            print("calulating hybrids...")
            hybrid1, hybrid2, hybrid5, nh2, nh5, nh10, ba = calc_all_hybrids(rec_1, rec_0, debug)
        if calc_hybrids:
            return auc, acc, prec_1, prec_0, rec_1, rec_0, f1_1, f1_0, hybrid1, hybrid2, hybrid5, nh2, nh5, nh10, ba, tn, fp, fn, tp
    else:
        return auc, acc, prec_1, prec_0, rec_1, rec_0, f1_1, f1_0, tn, fp, fn, tp

#def metrics_dict(auc, acc, prec_1, prec_0, rec_1, rec_0, f1_1, f1_0, hybrid1, hybrid2, nh1, nh2, tn, fp, fn, tp, metricset):
def metrics_dict(auc, acc, prec_1, prec_0, rec_1, rec_0, f1_1, f1_0, tn, fp, fn, tp, metricset):
    dictmetrics = { 'accuracy %s' % metricset: acc,
        'precision 1 %s' % metricset: prec_1,
        'recall 1 %s' % metricset: rec_1,
        'f1-score 1 %s' % metricset: f1_1,
        'precision 0 %s' % metricset: prec_0,
        'recall 0 %s' % metricset: rec_0,
        'f1-score 0 %s' % metricset: f1_0,
        'auc %s' % metricset: auc,
        'TN %s' % metricset: tn,
        'FP %s' % metricset: fp,
        'FN %s' % metricset: fn,
        'TP %s' % metricset: tp,
    }
    return dictmetrics

def metrics_dict_full(auc, acc, prec_1, prec_0, rec_1, rec_0, f1_1, f1_0, tn, fp, fn, tp, hybrid1, hybrid2, hybrid5, nh2, nh5, nh10, ba, metricset):
    fulldict = {**metrics_dict(auc, acc, prec_1, prec_0, rec_1, rec_0, f1_1, f1_0, tn, fp, fn, tp, metricset),
                **metrics_dict_hybrid(hybrid1, hybrid2, hybrid5, nh2, nh5, nh10, ba, metricset)}
    return fulldict

def metrics_dict_plus_distrib(auc, acc, prec_1, prec_0, rec_1, rec_0, f1_1, f1_0, tn, fp, fn, tp, fn01, fn02, fn001, fn002, metricset):
    fulldict = {**metrics_dict(auc, acc, prec_1, prec_0, rec_1, rec_0, f1_1, f1_0, tn, fp, fn, tp, metricset),
                **metrics_dict_distrib(fn01, fn02, fn001, fn002, metricset)}
    return fulldict

def metrics_dict_hybrid(hybrid1, hybrid2, hybrid5, nh2, nh5, nh10, ba, mset):
    hydriddict= {}
    hydriddict['hybrid1 %s' % mset] = hybrid1
    hydriddict['hybrid2 %s' % mset] = hybrid2
    hydriddict['hybrid5 %s' % mset] = hybrid5
    hydriddict['NH2 %s' % mset] = nh2
    hydriddict['NH5 %s' % mset] = nh5
    hydriddict['NH10 %s' % mset] = nh10
    hydriddict['BA %s' % mset] = ba
    return hydriddict

def metrics_dict_distrib(fn01, fn02, fn001, fn002, mset):
    distribdict= {}
    distribdict['FN01 %s' % mset] = fn01
    distribdict['FN02 %s' % mset] = fn02
    distribdict['FN001 %s' % mset] = fn001
    distribdict['FN002 %s' % mset] = fn002
    return distribdict

def calc_metrics_custom(tn, fp, fn, tp, y_scores=None, y=None, numaucthres=200, debug=True, calc_hybrids=False):
    if debug:
        print("calulating merics (custom)")
    if debug:
        print("(input) tn : %d, fp : %d, fn : %d, tp : %d" % (tn, fp, fn, tp))
    if debug:
        print("calulating auc...")
    if numaucthres > 0 and y is not None and y_scores is not None:
        aucmetric = AUC(num_thresholds=numaucthres)
        aucmetric.update_state(y, y_scores[:, 1])
        auc = float(aucmetric.result())
    else:
        auc = 0.0
    if debug:
        print("auc : %.2f" % auc)
    ##############################################
    # tp0 = tn1, tn0 = tp1, fp0 = fn1, fn0 = fp1 #
    ##############################################
    if debug:
        print("calulating accuracy...")
    acc = accuracy(tp, tn, fp, fn)
    #acc_0 = accuracy(tn, tp, fn, fp)
    if debug:
        print("accuracy : %.2f" % acc)
        #print("accuracy 0 : %.2f" % acc_0)
    if debug:
        print("calulating recall ...")
    rec_1 = recall(tp, fn)
    rec_0 = recall(tn, fp)
    if debug:
        print("recall 1 : %.2f" % rec_1)
        print("recall 0 : %.2f" % rec_0)
    if debug:
        print("calulating precision...")
    prec_1 = precision(tp, fp)
    prec_0 = precision(tn, fn)
    if debug:
        print("precision 1 : %.2f" % prec_1)
        print("precision 0 : %.2f" % prec_0)
    if debug:
        print("calulating f1_score...")
    f1_1 = f1(tp, fp, fn)
    f1_0 = f1(tn, fn, fp)
    if debug:
        print("f1 1 : %.2f" % f1_1)
        print("f1 0 : %.2f" % f1_0)

    if calc_hybrids:
        if debug:
            print("calulating hybrids ...")
        hybrid1, hybrid2, hybrid5, nh2, nh5, nh10, ba = calc_all_hybrids(rec_1, rec_0, debug)

    if calc_hybrids:
        return auc, acc, prec_1, prec_0, rec_1, rec_0, f1_1, f1_0, hybrid1, hybrid2, hybrid5, nh2, nh5, nh10, ba, tn, fp, fn, tp
    else:
        return auc, acc, prec_1, prec_0, rec_1, rec_0, f1_1, f1_0, tn, fp, fn, tp

def metrics_aggr(metrics, mean_metrics, hybrid_on_aggr=True, y_scores=None, y=None, valst=None):
    recall1s = []
    recall0s = []
    for m in metrics[0]:
        if isinstance(metrics[0][m], str):
            continue
        metricsum = sum([item.get(m, 0) for item in metrics if item.get(m) >= 0])
        cmvalsts = ['TN', 'FP', 'FN', 'TP']
        if any([st in m for st in cmvalsts]):
            mean_metrics[m] = metricsum
        else:
            mean_metrics[m] = metricsum / len(metrics)
        if 'recall 1' in m:
            recall1s.append(m)
        if 'recall 0' in m:
            recall0s.append(m)
    if hybrid_on_aggr:
        for i in range(0,len(recall1s)):
            hybrid1, hybrid2, hybrid5, nh2, nh5, nh10, ba = calc_all_hybrids(mean_metrics[recall1s[i]], mean_metrics[recall0s[i]])
            mset = re.search('(?<=recall 1 ).*$',recall1s[i]).group(0)
            hybrid_metrics_dict = metrics_dict_hybrid(hybrid1, hybrid2, hybrid5, nh2, nh5, nh10, ba, mset)
            mean_metrics = {**mean_metrics, **hybrid_metrics_dict}
    if y_scores is not None and y is not None and valst is not None:
        metrics_dict_dist = metrics_dict_distrib(*calc_all_model_distrib(y_scores, y), valst)
        mean_metrics = {**mean_metrics, **metrics_dict_dist}
    return mean_metrics

def metrics_aggr2(metrics, metricset):
    cf = {}
    cfvars=['%s %s'%(v,metricset) for v in ['TN', 'FP', 'FN', 'TP']]
    for m in cfvars:
        cf[m] = sum([item.get(m, 0) for item in metrics if item.get(m) >= 0])
    return metrics_dict(*calc_metrics_custom(*tuple([cf[m] for m in cf])), metricset)

