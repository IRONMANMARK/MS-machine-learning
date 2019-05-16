
'''This file is used to read in raw data file into a database
    Author: Zichen Liu'''
import tensorflow as tf
import os
import re
import numpy as np
from tqdm import tqdm
import sqlite3

# os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
# #
# #
# a = tf.constant([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], shape=[2, 3], name='a')
# b = tf.constant([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], shape=[3, 2], name='b')
# c = tf.matmul(a, b)
# sess = tf.Session(config=tf.ConfigProto(log_device_placement=True))
# print(sess.run(c))

# count = 0
# with open(file_l) as p:
#     for line_ in tqdm(p):
#         lines_ = line_.strip('\n')
#         if lines_ == 'BEGIN IONS':
#             count += 1
#         else:
#             continue
# print(count)

def read_library(file, database):
    '''
    This is the main function that used to read in the raw data file and
    :param file: raw data file form like "library.mgf"
    :param database: database file you want to create form like "database.db"
    :return: return a message said "successfully read the library"
    '''
    file_list = []
    # creat a database and connect to the database
    db = sqlite3.connect(database)
    cur = db.cursor()
    # allow the database perform dirty read
    db.execute('PRAGMA synchronous = OFF')
    # try to create a table name mgfData
    try:
        cur.execute('DROP TABLE mgfData')
        # db.execute('VACUUM')
        try:
            sql = '''create table mgfData (
                    pepmass real,
                    charge real,
                    fdr real,
                    m_z real,
                    intensity real,
                    score real,
                    protein text,
                    seq text,
                    seq_decorate text)'''
            db.execute(sql)
            print('Database has been successfully created!')
        except:
            print('Database has already been created!')
    except:
        try:
            sql = '''create table mgfData (
                    pepmass real,
                    charge real,
                    fdr real,
                    m_z real,
                    intensity real,
                    score real,
                    protein text,
                    seq text,
                    seq_decorate text)'''
            db.execute(sql)
            print('Database has been successfully created!')
        except:
            print('Database has already been created!')
    # read in the raw data as a file stream
    with open(file) as f:
        for line in tqdm(f):
            lines = line.strip('\n')
            if lines in ['BEGIN IONS', '']:
                continue
            elif lines == 'END IONS':
                mark = '='
                dic = dict()
                pattern = re.compile(r'=')
                pattern2 = re.compile(r'\t')
                num_list = []
                bb = []
                cc = []
                # parse the data out from the raw data, put the raw data in a dic
                for i in range(len(file_list)):
                    b = mark in file_list[i]
                    if b is True:
                        split = pattern.split(file_list[i])
                        dic[split[0]] = split[1]
                        # print(split, can_transfer_to_float(split[1]))
                    else:
                        num_sp = pattern2.split(file_list[i])
                        num_sp[0] = float(num_sp[0])
                        num_sp[1] = float(num_sp[1])
                        num_list.append(num_sp)
                # get value from the dic
                num_tp = tuple(num_list)
                num_mx = np.vstack(num_tp)
                pepmass = float(dic.get('PEPMASS'))
                charge = float(dic.get('CHARGE'))
                seq = dic.get('SEQ')
                protein = dic.get('PROTEIN')
                score = float(dic.get('SCORE'))
                fdr = float(dic.get('FDR'))
                for str in seq:
                    if str.isalpha() is True:
                        cc.append(str)
                    elif str.isdigit() is True:
                        bb.append(str)
                    elif str == '+':
                        bb.append(str)
                    else:
                        bb.append(str)
                num_str = ''.join(bb)
                alpha_str = ''.join(cc)
                # num_str = num_str.strip('+')
                mix = []
                # creat a matrix that will be put in to the database
                for i in range(len(num_mx)):
                    row = [pepmass, charge, fdr, num_mx[i][0], num_mx[i][1]]
                    result_row = [score, protein, alpha_str, num_str]
                    mix_row = row + result_row
                    mix.append(mix_row)
                fin_tp = tuple(mix)
                # put the data matrix in to the database
                # if there is an error the database will roll back
                # put a huge matrix into the database instead of putting just one
                try:
                    db.executemany("insert into mgfData values(?,?,?,?,?,?,?,?,?)", (fin_tp))
                except:
                    print('Rolling back......')
                    err = 'There is an error occur !'
                    db.rollback()
                    return err
                file_list = []
                continue
            else:
                file_list.append(lines)
        # commit the change to the database if there is an error occur roll back
        try:
            db.commit()
        except:
            print('Rolling back......')
            err = 'There is an error occur !'
            db.rollback()
            return err

    db.close()
    successed = 'Successfully read the library!'
    return successed

# db = sqlite3.connect(database)
# db.execute("create index seq_index on mgfData (seq)")
# db.close()
# # print(read_library(file_l,database))
# end = datetime.datetime.now()
# print((end-start))
if __name__ == "__main__":
    file = 'lib.mgf'
    database = 'test.db'
    print(read_library(file, database))
