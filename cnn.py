
'''
This is the main script for CNN
Author: Zichen Liu
'''
import sqlite3
from tqdm import tqdm
import numpy as np
import keras
from keras.models import Sequential
from keras.layers import Dense, Conv1D, Flatten, MaxPooling1D, GlobalAveragePooling1D, Dropout, Activation, Convolution1D
from keras.optimizers import SGD

def get_amino_dic(database):
    '''
    This is the function will get the aminoacid dic
    This function will not be called in this script
    :param database: The database you want to generated this dic
    :return: return the dic
    '''
    db = sqlite3.connect(database)
    aminoacid = list(db.execute("select * from amino_acid"))
    amino_dic = dict()
    for i in range(len(aminoacid)):
        amino_dic[aminoacid[i][0]] = i+1
    return amino_dic



def get_deco_list(database):
    '''
    This is the function will get decoration sequence parsed into individual number
    :param database: the database you want to generated this set
    :return: return the set of individual decoration sequence number
    '''
    db = sqlite3.connect(database)
    deco_list = list(db.execute("select * from seq_deco"))
    a_set = set()
    # parse the decoration sequence to individual number
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
    '''
    This is the main function for training CNN and write the result to a file
    :param database: the database you want to get data from
    :return: no return
    '''
    # get the result label list
    deco_list = sorted(list(get_deco_list(database)))
    db2 = sqlite3.connect(database)
    seq_list = list(db2.execute("select * from filter_seq"))
    print(deco_list, deco_list.index('+57.021'))
    one_hot = []
    x_train = []
    y_train = []
    x_fit = []
    y_fit = []
    file2 = open('result.txt', 'w')
    # create CNN model
    model = Sequential()

    model.add(Convolution1D(nb_filter=128, filter_length=1, input_shape=(4684, 1)))
    model.add(Activation('relu'))
    model.add(Flatten())
    model.add(Dropout(0.4))
    model.add(Dense(512, activation='relu'))
    model.add(Dense(256, activation='relu'))
    model.add(Dense(7))
    model.add(Activation('softmax'))
    sgd = SGD(lr=0.01, nesterov=True, decay=1e-6, momentum=0.9)
    model.compile(loss='categorical_crossentropy', optimizer=sgd, metrics=['accuracy'])
    model.summary()
    # get data from the database and train it.
    for i in range(len(seq_list)):
        count = i
        data_list = list(db2.execute("select pepmass, score, charge, m_z, intensity from random where seq = ?", (seq_list[i])))
        result_list = list(db2.execute("select seq_decorate from random where seq = ?", (seq_list[i])))
        a = result_list[0][0]
        a_list = []
        num_list = []
        a_final = []
        # parse the data and put data in the training matrix and result matrix
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
        if count < int(len(seq_list) * 0.9):
            on_hot_sub = [0, 0, 0, 0, 0, 0, 0]
            for i in range(len(b)):
                on_hot_sub[deco_list.index(b[i])] = 1
            # y_train.append(np.expand_dims(np.array(on_hot_sub), axis=1))
            # length_list.append(len(data_list))
            data_mz = [0 for i in range(4684)]
            for i in data_list:
                data_mz[int(i[3]) - 55] = i[4]
            # x_train.append(np.expand_dims(np.array(data_mz), axis=1))
            # print(y_train2)
            # model.fit(x_train2, y_train2, epochs=15, verbose=1)
            print(np.array([np.expand_dims(np.array(data_mz), axis=1)]).shape)
            print(np.expand_dims(np.array(on_hot_sub), axis=0).shape)
            # train the matrix
            model.fit(np.array([np.expand_dims(np.array(data_mz), axis=1)]),
                      np.expand_dims(np.array(on_hot_sub), axis=0), epochs=15, verbose=1)
        else:
            # this is the 10% for testing the data
            on_hot_sub = [0, 0, 0, 0, 0, 0, 0]
            for i in range(len(b)):
                on_hot_sub[deco_list.index(b[i])] = 1
            y_fit.append(np.expand_dims(np.array(on_hot_sub), axis=1))
            # length_list.append(len(data_list))
            data_mz = [0 for i in range(4684)]
            for i in data_list:
                data_mz[int(i[3]) - 55] = i[4]
            x_fit.append(np.expand_dims(np.array(data_mz), axis=1))
            result = model.evaluate(np.array([np.expand_dims(np.array(data_mz), axis=1)]),
                      np.expand_dims(np.array(on_hot_sub), axis=0))
            file2.write(result)


if __name__ == "__main__":
    print(deco_cnn_result_list('test4.db'))