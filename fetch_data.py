import sqlite3
import datetime
import re
import tqdm
from ctypes import *
import multiprocessing
import random
from multiprocessing import Process
from threading import Thread


start = datetime.datetime.now()
database = 'test.db'
database2 = 'test3.db'
def generate_seq_list(database):
    db = sqlite3.connect(database)
    cur = db.cursor()
    try:
        cur.execute('DROP TABLE filter_seq')
        # db.execute('VACUUM')
        try:
            sql = '''create table filter_seq (
                    seq text)'''
            db.execute(sql)
            print('Database has been successfully created!')
        except:
            print('Database has already been created!')
    except:
        try:
            sql = '''create table filter_seq (
                    seq text)'''
            db.execute(sql)
            print('Database has been successfully created!')
        except:
            print('Database has already been created!')

    db.execute("insert into filter_seq select seq from random group by seq having count (*) > 2 order by seq")
    # print(seq_tuple)
    # db.executemany("insert into filter_seq values (?)", (seq_list))
    db.commit()
    db.close()
    suess = 'Generation complete ! '
    return suess

def generate_seq_decorate_list(database, table_name):
    db = sqlite3.connect(database)
    cur = db.cursor()
    try:
        cur.execute('DROP TABLE seq_deco')
        # db.execute('VACUUM')
        try:
            sql = '''create table seq_deco (
                    seq text)'''
            db.execute(sql)
            print('Database has been successfully created!')
        except:
            print('Database has already been created!')
    except:
        try:
            sql = '''create table seq_deco (
                    seq text)'''
            db.execute(sql)
            print('Database has been successfully created!')
        except:
            print('Database has already been created!')

    db.execute("insert into seq_deco select seq_decorate from %s group by seq_decorate having count (*) > 2 order by seq_decorate" %table_name)
    # print(seq_tuple)
    # db.executemany("insert into filter_seq values (?)", (seq_list))
    db.commit()
    db.close()
    suess = 'Generation complete! '
    return suess


def purge_numerical_data(database):
    db = sqlite3.connect(database)
    cur = db.cursor()
    try:
        cur.execute('DROP TABLE filter_seq_deco2')
        cur.execute('DROP TABLE filter_seq_deco3')
        # db.execute('VACUUM')
        try:
            sql = '''create table filter_seq_deco2 (
                    seq real)'''
            sql2 = '''create table filter_seq_deco3 (
                    seq real)'''
            db.execute(sql2)
            db.execute(sql)
            print('Database has been successfully created!')
        except:
            print('Database has already been created!')
    except:
        try:
            sql = '''create table filter_seq_deco2 (
                    seq real)'''
            sql2 = '''create table filter_seq_deco3 (
                    seq real)'''
            db.execute(sql2)
            db.execute(sql)
            print('Database has been successfully created!')
        except:
            print('Database has already been created!')
    a = list(db.execute("select * from filter_seq_deco"))
    c = []
    b = []
    for i in range(len(a)):
        p = a[i][0].strip('-')
        aa = '-' in p
        bb = '+' in p
        if (aa ==  True and bb == False):
            cc = p.split('-')
        elif (aa == False and bb == True):
            cc = p.split('+')
        elif (aa == True and bb == True):
            cc = re.split('[-+]', p)
        else:
            continue
        for ii in range(len(cc)):
            c.append(cc[ii])
    for i in c:
        hh = float(i)
        b.append(hh)
    n = 1
    d = [b[i:i+n] for i in range(0, len(b), n)]
    db.executemany("insert into filter_seq_deco2 values (?)", (d))
    db.commit()
    db.execute("insert into filter_seq_deco3 select seq from filter_seq_deco2 group by seq having count (*) > 2 order by seq")
    db.commit()
    db.close()
    sucess = 'Done! '
    return sucess
def break_to_aminoacid(database):
    db = sqlite3.connect(database)
    cur = db.cursor()
    try:
        cur.execute('DROP TABLE amino_acid')
        cur.execute('DROP TABLE amino_acid_pre')
        # db.execute('VACUUM')
        try:
            sql = '''create table amino_acid (
                    seq text)'''
            sql2 = '''create table amino_acid_pre (
                    seq text)'''
            db.execute(sql)
            db.execute(sql2)
            print('Database has been successfully created!')
        except:
            print('Database has already been created!')
    except:
        try:
            sql = '''create table amino_acid (
                    seq text)'''
            sql2 = '''create table amino_acid_pre (
                    seq text)'''
            db.execute(sql)
            db.execute(sql2)
            print('Database has been successfully created!')
        except:
            print('Database has already been created!')
    seq_list = list(db.execute("select * from filter_seq"))
    a = []
    for i in range(len(seq_list)):
        split_seq = seq_list[i][0]
        for i in split_seq:
            a.append(i)
    db.executemany("insert into amino_acid_pre values (?)", (a))
    db.commit()
    db.execute("insert into amino_acid select seq from amino_acid_pre group by seq having count (*) > 2 order by seq")
    db.commit()
    cur.execute('DROP TABLE amino_acid_pre')
    db.close()
    sucess = 'Done! '
    return sucess


def fetch_data(database):
    sqlite3.threadsafety = 2
    db = sqlite3.connect(database, isolation_level=None, check_same_thread=False)
    cur = db.cursor()
    db.execute("PRAGMA read_uncommitted = TRUE")
    db.execute("pragma journal_mode=wal")
    cores = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=cores)
    # lib = cdll.LoadLibrary("libdead_loop.so")
    seq_list = list(db.execute("select * from amino_acid"))
    sucess = 'Done! '
    long = len(seq_list)
    for i in range(long):
        sss = seq_list[i][0]
        pool.apply_async(sub_process(sss, db, cur), args=(sss, db, cur))
    pool.close()
    pool.join()
    db.close()
    return sucess

def sub_process(seq_list, db, cur):
    numm = seq_list
    table = []
    table.append(numm)
    fin = ''.join(table)
    try:
        cur.execute('DROP TABLE %s' % fin)
        # db.execute('VACUUM')
        try:
            sql = '''create table %s (
                    pepmass real,
                    charge real,
                    fdr real,
                    m_z real,
                    intensity real,
                    score real,
                    protein text,
                    seq text,
                    seq_decorate text)'''
            db.execute(sql % fin)
            # print('Database has been successfully created!')
        except:
            print('ERROR in Creating database')
    except:
        try:
            sql = '''create table %s (
                    pepmass real,
                    charge real,
                    fdr real,
                    m_z real,
                    intensity real,
                    score real,
                    protein text,
                    seq text,
                    seq_decorate text)'''
            db.execute(sql % fin)
            # print('Database has been successfully created!')
        except:
            print('ERROR in Creating database')
    # print(seq_list[0][0])
    db.execute("insert into " + fin + " select * from mgfData where seq like  '%" + seq_list + "%'")
    db.commit()
    sucess = 'Done! '
    return sucess
# print(break_to_aminoacid(database))

#
# if __name__ == '__main__':
#     print(fetch_data(database))
#
# print(generate_seq_decorate_list(database))
# db = sqlite3.connect(database)
# cur = db.cursor()
# cur.execute('drop table filter_seq_deco')

def get_50000(database, database2):
    db = sqlite3.connect(database)
    cur = db.cursor()
    db2 = sqlite3.connect(database2)
    cur2 = db2.cursor()
    try:
        cur2.execute('DROP TABLE random')
        # db.execute('VACUUM')
        try:
            sql = '''create table random (
                    pepmass real,
                    charge real,
                    fdr real,
                    m_z real,
                    intensity real,
                    score real,
                    protein text,
                    seq text,
                    seq_decorate text)'''
            db2.execute(sql)
            print('Database has been successfully created!')
        except:
            print('Database has already been created!')
    except:
        try:
            sql = '''create table random (
                    pepmass real,
                    charge real,
                    fdr real,
                    m_z real,
                    intensity real,
                    score real,
                    protein text,
                    seq text,
                    seq_decorate text)'''
            db2.execute(sql)
            print('Database has been successfully created!')
        except:
            print('Database has already been created!')
    full_list = list(db.execute('select * from filter_seq'))
    short_list = random.sample(full_list, 5000)
    # print(short_list)
    for i in tqdm.tqdm(range(len(short_list))):
        a = list(db.execute('select * from mgfData where seq = ?', (short_list[i])))
        db2.executemany('insert into random values(?,?,?,?,?,?,?,?,?)', (a))
    # db2.executemany('insert into random select * from  where seq = ?', (short_list))
    db2.commit()
    db2.close()
    db.close()
    success = "Done!"
    return success

# print(purge_numerical_data(database2))
# db = sqlite3.connect(database2)
# db.execute('drop table filter_seq_deco')
# db.execute('drop table filter_seq_deco2')
# print(get_50000(database, database2))
end = datetime.datetime.now()
print(end - start)

def clear_all(database):
    db = sqlite3.connect(database)
    cur = db.cursor()
    seq_list = list(db.execute("select * from amino_acid"))
    for i in range(len(seq_list)):
        table = []
        table.append(seq_list[i][0])
        fin = ''.join(table)
        try:
            cur.execute('DROP TABLE %s' % fin)
        except:
            continue
# print(clear_all(database))

if __name__ == "__main__":
    database = 'test2.db'
    # get_50000('test.db', 'test4.db')
    # generate_seq_list('test4.db')
    # generate_seq_decorate_list(database, 'random')
    purge_numerical_data(database)