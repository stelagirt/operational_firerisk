from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier
from xgboost import XGBClassifier, Booster
from keras import Sequential
from keras.layers import Dense, Dropout
from keras.models import load_model
import keras.metrics
import numpy as np
from keras.optimizers import Adam, SGD
from keras.callbacks import EarlyStopping, TensorBoard
from MLscores import calc_metrics, metrics_dict, cmvals, recall, hybridrecall
import keras.backend as K
import tensorflow as tf
import pickle


def limitgpumem(MBs):
    gpus = tf.config.experimental.list_physical_devices('GPU')
    if gpus:
      # Restrict TensorFlow to only allocate MBs of memory on the first GPU
      try:
        tf.config.experimental.set_virtual_device_configuration(
            gpus[0],
            [tf.config.experimental.VirtualDeviceConfiguration(memory_limit=MBs)]) # Notice here
        logical_gpus = tf.config.experimental.list_logical_devices('GPU')
        print(len(gpus), "Physical GPUs,", len(logical_gpus), "Logical GPUs")
      except RuntimeError as e:
        # Virtual devices must be set before GPUs have been initialized
        print(e)

def allowgrowthgpus():
    gpus = tf.config.list_physical_devices('GPU')
    if gpus:
      try:
        # Currently, memory growth needs to be the same across GPUs
        for gpu in gpus:
          tf.config.experimental.set_memory_growth(gpu, True)
        logical_gpus = tf.config.list_logical_devices('GPU')
        print(len(gpus), "Physical GPUs,", len(logical_gpus), "Logical GPUs")
      except RuntimeError as e:
        # Memory growth must be set before GPUs have been initialized
        print(e)

def run_predict_q(model, modeltype, X, q):
    y_scores, y_pred = run_predict(model, modeltype, X)
    q.put(y_scores)
    q.put(y_pred)

def run_predict(model, modeltype, X):
    if modeltype == 'tf':
        y_scores = model.predict(X, verbose=0 )
    elif modeltype == 'sk':
        y_scores = model.predict_proba(X)
    predict_class = lambda p: int(round(p))
    predict_class_v = np.vectorize(predict_class)
    y_pred = predict_class_v(y_scores[:, 1])
    return y_scores, y_pred

def run_predict_and_metrics_q(modeltype, X, y, metricset, dontcalc=False, numaucthres=200, q=None):
    model=load_model("currentfit")
    metricsdict, y_scores = run_predict_and_metrics(model, modeltype, X, y, metricset, dontcalc, numaucthres)
    if q is not None:
        q.put(metricsdict)
        q.put(y_scores)

def run_predict_and_metrics(model, modeltype, X, y, metricset, dontcalc=False, numaucthres=200):
    if dontcalc:
        zerostuple = tuple([0]*16)
        return metrics_dict(*zerostuple, metricset)
    y_scores, y_pred = run_predict(model, modeltype, X)
    metricsdict = metrics_dict(*calc_metrics(y, y_scores, y_pred, numaucthres=numaucthres), metricset)
    return metricsdict, y_scores

def create_and_fit_q(modeltype, params, X_train, y_train, X_val, y_val, q):
    #if xlaflags is not None: os.environ["XLA_FLAGS"] = xlaflags
    model, res = create_and_fit(modeltype, params, X_train, y_train, X_val, y_val)
    model.save('currentfit')
    q.put(res)

def create_and_fit(modeltype, params, X_train, y_train, X_val=None, y_val=None):
    model = create_model(modeltype, params, X_train)
    model, res = fit_model(modeltype, model, params, X_train, y_train, X_val, y_val)
    return model,res

def create_model(modeltype, params, X_train):
    if modeltype == 'tf':
        model = create_NN_model(params, X_train)
    elif modeltype == 'sk':
        model = create_sklearn_model(params, X_train)
    elif modeltype == 'ensemble':
        model = create_ensemble_model(params, X_train)
    return model

def create_ensemble_model(params, X_train):
    pass

def mm_save_model(fname, model, modeltype, params):
    if modeltype == 'tf':
        model.save(fname)
    elif modeltype == 'sk':
        if params['algo']!='XGB':
            pickle.dump(model, open(fname, "wb"))
        else:
            model.save_model(fname)

def mm_save_weights(fname, model):
     model.save_weights(fname)

def mm_load_weights(fname, model):
     model.load_weights(fname)

def mm_load_model(fname, modeltype=None, params=None):
    if modeltype is None: modeltype='tf'
    if modeltype == 'tf':
        #model=create_model(modeltype, params, X_train)
        #model.load_weights(fname)
        model = load_model(fname)
    elif modeltype == 'sk':
        if params['algo']!='XGB':
            model= pickle.load(open(fname, "rb"))
        else:
            model = Booster()
            model.load_model(fname)
    return model

def fit_model(modeltype, model, params, X_train, y_train, X_val=None, y_val=None):
    if modeltype == 'tf':
        es = EarlyStopping(monitor=params['ES_monitor'], patience=params['ES_patience'], min_delta=params['ES_mindelta'])
        if X_val is not None and y_val is not None:
            res = model.fit(X_train, y_train, batch_size=params['batch_size'], epochs=params['max_epochs'], verbose=0, callbacks=[es],
                            validation_data=(X_val, y_val), class_weight=params['class_weights'])
        else:
            res = model.fit(X_train, y_train, batch_size=params['batch_size'], epochs=params['max_epochs'], verbose=0, callbacks=[es],
                            class_weight=params['class_weights'])
    elif modeltype == 'sk':
        res = model.fit(X_train, y_train)
    return model, res

def recall_loss(y_true, y_pred):
    # recall of class 1
    y_true = K.cast(y_true, tf.float32)
    #do not use "round" here if you're going to use this as a loss function
    true_positives = K.sum(y_pred * y_true)
    possible_positives = K.sum(y_true)
    return true_positives / (possible_positives + K.epsilon())

def unbalanced_loss(y_true, y_pred):

    # under construction...

    #y_true = K.cast(y_true, tf.float32)
    y_true = K.cast(y_true, tf.float32)
    #y_pred = K.cast(y_pred, tf.int64)
    custom_loss = K.square(y_true - y_pred)
    #custom_loss = K.sum(y_pred - y_true)
    return custom_loss

'''
    tp = K.sum(y_pred * y_true)
    fn = K.sum((1-y_pred) * y_true)
    tn = K.sum((1-y_pred) * (1-y_true))
    fp = K.sum(y_pred * (1-y_true))

    rec_1 = recall(tp, fn)
    rec_0 = recall(tn, fp)
    nh2 = hybridrecall(2, 1, rec_1, rec_0, 'NH')
    #nh5 = hybridrecall(5, 1, rec_1, rec_0, 'NH')
    return -nh2
'''

def create_NN_model(params, X):
    # initializer
    #initializer = initializers.Constant(0.5)
    # define model
    model = Sequential()
    n_features = X.shape[1]
    intlayers = int(params['n_internal_layers'][0])
    model.add(Dense(params['n_internal_layers'][1]['layer_1_' + str(intlayers) + '_nodes'], activation='relu', input_shape=(n_features,))) #kernel_initializer=initializer))
    if not params['dropout'] is None:
        model.add(Dropout(params['dropout']))
    for i in range(2, intlayers + 2):
        model.add(Dense(int(params['n_internal_layers'][1]['layer_' + str(i) + '_' + str(intlayers) + '_nodes']),
                        activation='relu', )) #kernel_initializer=initializer))
        if not params['dropout'] is None:
            model.add(Dropout(params['dropout']))

        # model.add(Dense(1, activation='sigmoid'))
    model.add(Dense(2, activation='softmax'))

    # compile the model

    if params['optimizer']['name']=='Adam':
        # adam = Adam(learning_rate=0.0001, beta_1=0.9, beta_2=0.999, amsgrad=False)
        if params['optimizer']['adam_params'] is None:
            opt = Adam()
        else:
            opt = Adam(learning_rate=params['optimizer']['adam_params']['learning_rate_adam'], beta_1=params['optimizer']['adam_params']['beta_1'],
                       beta_2=params['optimizer']['adam_params']['beta_2'],amsgrad=params['optimizer']['adam_params']['amsgrad'])
    elif params['optimizer']['name']=='SGD':
        opt = SGD(learning_rate=params['optimizer']['learning_rate_SGD'])

    if params['metric'] == 'accuracy':
        metrics = ['accuracy']
    elif params['metric'] == 'sparse':
        metrics = [tf.metrics.SparseCategoricalAccuracy()]
    elif params['metric'] == 'tn':
        metrics = [tf.metrics.TrueNegatives(),tf.metrics.TruePositives()]
    if 'loss' in params and params['loss'] == 'unbalanced':
        lossf=unbalanced_loss
    else:
        lossf='sparse_categorical_crossentropy'
    model.compile(optimizer=opt, loss=lossf, metrics=metrics)  # , AUC(multi_label=False)])
    # model.compile(optimizer=opt, loss=recallloss, metrics=metrics)
    return model

def create_sklearn_model(params, X):
    n_features = X.shape[1]
    if params['algo']=='RF':
        max_feat = int(n_features/10*params['max_features'])
        model = RandomForestClassifier(max_depth=params['max_depth'], n_estimators=params['n_estimators'], min_samples_split=params['min_samples_split'], \
                                       min_samples_leaf=params['min_samples_leaf'],criterion=params['criterion'],max_features=max_feat,
                                       bootstrap=params['bootstrap'], class_weight=params['class_weights'], n_jobs = 8
                                       )
    if params['algo']=='XT':
        max_feat = int(n_features/10*params['max_features'])
        model = ExtraTreesClassifier(max_depth=params['max_depth'], n_estimators=params['n_estimators'], min_samples_split=params['min_samples_split'], \
                                       min_samples_leaf=params['min_samples_leaf'],criterion=params['criterion'],max_features=max_feat,
                                       bootstrap=params['bootstrap'], class_weight=params['class_weights'], n_jobs = 8
                                       )
    if params['algo']=='XGB':
        model = XGBClassifier(max_depth=int(params['max_depth']), n_estimators=params['n_estimators'], subsample=params['subsample'],\
                              reg_alpha=params['alpha'], gamma=params['gamma'], reg_lambda=params['lambda'],\
                              scale_pos_weight=params['scale_pos_weight'], n_jobs=8)

    return model