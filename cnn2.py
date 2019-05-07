import sqlite3
from tqdm import tqdm
import numpy as np
import keras
from keras.models import Sequential
from keras.layers import Dense, Conv1D, Flatten, MaxPooling1D, GlobalAveragePooling1D, Dropout, Activation, Convolution1D
from keras.optimizers import SGD

def get_amino_dic(database):
    db = sqlite3.connect(database)
    aminoacid = list(db.execute("select * from amino_acid"))
    amino_dic = dict()
    for i in range(len(aminoacid)):
        amino_dic[aminoacid[i][0]] = i+1
    return amino_dic

def get_amino_result_matrix(dic, database2):
    db2 = sqlite3.connect(database2)
    cur2 = db2.cursor()
    seq_list = list(db2.execute("select * from filter_seq"))
    result_list =[]
    long = []
    for i in range(len(seq_list)):
        one_result = []
        a = seq_list[i][0]
        long.append(len(seq_list[i][0]))
        for ii in range(len(a)):
            one_result.append(dic.get(a[ii]))
        result_list.append(one_result)
    print( min(long), max(long))
    long2 = []
    each = []
    for i in tqdm(range(len(seq_list))):
        data_list = list(db2.execute("select pepmass, charge, m_z, intensity from mgfData where seq = ?", (seq_list[i])))
        long2.append(len(data_list))
        each.append(len(data_list) / len(seq_list[i][0]))
    print(min(long2), max(long2), sum(long2), len(long2))
    print(min(each), max(each))
    return 0

def get_deco_list(database):
    db = sqlite3.connect(database)
    deco_list = list(db.execute("select * from seq_deco"))
    a_set = set()
    for deco in deco_list:
        a = str(deco[0])
        a_list = []
        num_list = []
        a_final = []
        for i in a:
            a_list.append(i)
        for i in range(len(a_list)):
            if (a_list[i] == '+' or a_list[i] == '-'):
                num_list.append(i + 1)
            else:
                continue
        for i in range(len(num_list)):
            a_list.insert(num_list[i] + i - 1, '\n')
        a_str = ''.join(a_list)
        a_final.append(a_str)
        b = a_final[0].split('\n')
        for i in b:
            a_set.add(i)
    return a_set

def deco_cnn_result_list(database):
    deco_list = sorted(list(get_deco_list(database)))
    db2 = sqlite3.connect(database)
    seq_list = list(db2.execute("select * from filter_seq"))
    print(deco_list, deco_list.index('+57.021'))
    one_hot = []
    x_train = []
    y_train = []
    x_fit = []
    y_fit = []
    length = set([])
    model = Sequential()

    model.add(Convolution1D(nb_filter=512, filter_length=1, input_shape=(4684, 5)))
    model.add(Activation('relu'))
    model.add(Flatten())
    model.add(Dropout(0.4))
    model.add(Dense(2048, activation='relu'))
    model.add(Dense(1024, activation='relu'))
    model.add(Dense(7))
    model.add(Activation('softmax'))
    sgd = SGD(lr=0.01, nesterov=True, decay=1e-6, momentum=0.9)
    model.compile(loss='categorical_crossentropy', optimizer=sgd, metrics=['accuracy'])
    model.summary()
    # model = Sequential()
    # model.add(Conv1D(512, 3, input_shape=(4684, 1)))
    # model.add(Activation('relu'))
    # model.add(Dropout(0.4))
    # model.add(Flatten())
    # model.add(Dense(2048, activation='relu'))
    # model.add(Dense(1024, activation='relu'))
    # model.add(Dense(7))
    # model.add(Activation('softmax'))
    # sgd = SGD(lr=0.01, nesterov=True, decay=1e-6, momentum=0.9)
    # model.compile(loss='binary_crossentropy',
    #               optimizer=sgd,
    #               metrics=['accuracy'])
    # print(model.summary())
    for i in tqdm(range(len(seq_list))):
        count = i
        data_list = list(db2.execute("select pepmass, score, charge, m_z, intensity from random where seq = ?", (seq_list[i])))
        result_list = list(db2.execute("select seq_decorate from random where seq = ?", (seq_list[i])))
        a = result_list[0][0]
        a_list = []
        num_list = []
        a_final = []
        for i in a:
            a_list.append(i)
        for i in range(len(a_list)):
            if (a_list[i] == '+' or a_list[i] == '-'):
                num_list.append(i + 1)
            else:
                continue
        for i in range(len(num_list)):
            a_list.insert(num_list[i] + i - 1, '\n')
        a_str = ''.join(a_list)
        a_final.append(a_str)
        b = a_final[0].split('\n')
        length.add(len(data_list))
        if count < int(len(seq_list) * 0.9):
            on_hot_sub = [0, 0, 0, 0, 0, 0, 0]
            for i in range(len(b)):
                on_hot_sub[deco_list.index(b[i])] = 1
            # y_train.append(np.expand_dims(np.array(on_hot_sub), axis=1))
            # length_list.append(len(data_list))
            data_mz = [0 for i in range(4684)]
            x_train_huge = []
            for i in data_list:
                x_train_huge.append(np.array(i))
            length2 = len(x_train_huge)
            for i in range(4684 - length2):
                x_train_huge.append(np.array(np.zeros(5)))
            print(len(x_train_huge))
            print(np.mat(x_train_huge))
            # break
            # x_train.append(np.expand_dims(np.array(data_mz), axis=1))
            # print(y_train2)
            # model.fit(x_train2, y_train2, epochs=15, verbose=1)
            # print(np.array([np.expand_dims(np.array(data_mz), axis=1)]).shape)
            # print(np.expand_dims(np.array(on_hot_sub), axis=0).shape)
            # # break
            model.fit(np.array([np.mat(x_train_huge)]),
                      np.expand_dims(np.array(on_hot_sub), axis=0), epochs=15, verbose=1)
            # print(np.expand_dims(np.array(data_mz), axis=1).shape)
        else:
            on_hot_sub = [0, 0, 0, 0, 0, 0, 0]
            for i in range(len(b)):
                on_hot_sub[deco_list.index(b[i])] = 1
            y_fit.append(np.expand_dims(np.array(on_hot_sub), axis=1))
            # length_list.append(len(data_list))
            data_mz = [0 for i in range(4684)]
            x_fit_huge = []
            for i in data_list:
                x_fit_huge.append(np.array(i))
            length3 = len(x_fit_huge)
            for i in range(4684 - length3):
                x_fit_huge.append(np.array(np.zeros(5)))
            # result = model.evaluate(x_fit, y_fit)
        # print(data_list)
    x_train = np.array(x_train)
    y_train = np.array(y_train)
    x_fit = np.array(x_fit)
    y_fit = np.array(y_fit)
    # print(x_train.shape)
    # CNN_core(x_train, y_train, x_fit, y_fit, 4684)

def CNN_core(x_train, y_train, x_fit, y_fit, seq_length, epochs=15):
    model = Sequential()
    model.add(Conv1D(64, 3, activation='relu', input_shape=(seq_length, 1)))
    model.add(Conv1D(64, 3, activation='relu'))
    model.add(MaxPooling1D(3))
    model.add(Conv1D(128, 3, activation='relu'))
    model.add(Conv1D(128, 3, activation='relu'))
    model.add(GlobalAveragePooling1D())
    model.add(Dropout(0.5))
    model.add(Dense(1, activation='sigmoid'))
    sgd = SGD(lr=0.01, nesterov=True, decay=1e-6, momentum=0.9)
    model.compile(loss='binary_crossentropy',
                  optimizer=sgd,
                  metrics=['accuracy'])
    print(model.summary())
    model.fit(x_train, y_train, epochs=epochs, verbose=1)
    result = model.evaluate(x_fit, y_fit)
    print(result)
    # model.fit(x_train, y_train, batch_size=16, epochs=10)
    # score = model.evaluate(x_test, y_test, batch_size=16)




if __name__ == "__main__":
    print(deco_cnn_result_list('test3.db'))
    # a = np.array([1,2,3,4])
    # print(np.expand_dims(a, axis=1))
    # CNN_core(4684)
    # get_amino_result_matrix(get_amino_dic('test.db'), 'test.db')
    # a = get_amino_dic('test.db')
    # print(a)
    # print(get_result_matrix(a, 'test.db'))
    # db = sqlite3.connect("test3.db")
    # db.execute("create index seq_index on random (seq)")
    # db.close()
    # db = sqlite3.connect('test.db')
    # db.execute("create index seq_index on mgfData (seq)")