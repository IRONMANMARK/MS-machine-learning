import tensorflow as tf
import os
from pyteomics import mgf
import re
import numpy as np
from tqdm import tqdm
import datetime
import sqlite3
import numba

# os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
# #
# #
# a = tf.constant([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], shape=[2, 3], name='a')
# b = tf.constant([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], shape=[3, 2], name='b')
# c = tf.matmul(a, b)
# sess = tf.Session(config=tf.ConfigProto(log_device_placement=True))
# print(sess.run(c))

start = datetime.datetime.now()
file_s = 'onelib.mgf'
file_l = 'LIBRARY.mgf'
file = 'lib.mgf'
# f = open(file)
database = 'test.db'
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
    file_list = []
    db = sqlite3.connect(database)
    cur = db.cursor()
    db.execute('PRAGMA synchronous = OFF')
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
    with open(file) as f:
        for line in tqdm(f):
            lines = line.strip('\n')
            if lines =='BEGIN IONS':
                continue
            elif lines == '':
                continue
            elif lines == 'END IONS':
                mark = '='
                dic = dict()
                pattern = re.compile(r'=')
                pattern2 = re.compile(r'\t')
                num_list = []
                bb = []
                cc = []
                for i in range(len(file_list)):
                    b = mark in file_list[i]
                    if b == True:
                        split = pattern.split(file_list[i])
                        dic[split[0]] = split[1]
                        # print(split, can_transfer_to_float(split[1]))
                    else:
                        num_sp = pattern2.split(file_list[i])
                        num_sp[0] = float(num_sp[0])
                        num_sp[1] = float(num_sp[1])
                        num_list.append(num_sp)
                num_tp = tuple(num_list)
                num_mx = np.vstack(num_tp)
                pepmass = float(dic.get('PEPMASS'))
                charge = float(dic.get('CHARGE'))
                seq = dic.get('SEQ')
                protein = dic.get('PROTEIN')
                score = float(dic.get('SCORE'))
                fdr = float(dic.get('FDR'))
                for str in seq:
                    if str.isalpha() == True:
                        cc.append(str)
                    elif str.isdigit() == True:
                        bb.append(str)
                    elif str == '+':
                        bb.append(str)
                    else:
                        bb.append(str)
                num_str = ''.join(bb)
                alpha_str = ''.join(cc)
                # num_str = num_str.strip('+')
                mix = []
                for i in range(len(num_mx)):
                    row = [pepmass, charge, fdr, num_mx[i][0], num_mx[i][1]]
                    result_row = [score, protein, alpha_str, num_str]
                    mix_row = row + result_row
                    mix.append(mix_row)
                fin_tp = tuple(mix)
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

db = sqlite3.connect(database)
db.execute("create index seq_index on mgfData (seq)")
db.close()
# print(read_library(file_l,database))
end = datetime.datetime.now()
print((end-start))

