
'''
This is the file that will fetch data from the database and create unique peptide sequence
 and peptide decoration sequence index
 Author: Zichen Liu
'''
import sqlite3
import datetime
import re
import tqdm
import multiprocessing
import random


def generate_seq_list(database, table_name):
    '''
    This is the function that will generate the unique peptide sequence index table.
    :param database: The database that you want to have a peptide sequence index table form like "test.db"
    :param table_name: The table name for the table that contain the raw data.
                        (Due to the table name I named for different database, This variable is required)
    :return: return a message said "Generation completed!"
    '''
    db = sqlite3.connect(database)
    cur = db.cursor()
    # create a table that will contain the unique peptide sequence
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
    # This is call for SQL that will filter out the unique sequence and put it in the newly created table
    db.execute("insert into filter_seq select seq from %s group by seq having count (*) > 2 order by seq" % table_name)
    db.commit()
    db.close()
    suess = 'Generation complete ! '
    return suess

def generate_seq_decorate_list(database, table_name):
    '''
    This is the function that will generate the unique peptide decoration sequence index table.
    :param database: The database that you want to have a peptide sequence index table form like "test.db"
    :param table_name: The table name for the table that contain the raw data.
                        (Due to the table name I named for different database, This variable is required)
    :return: return a message said "Generation completed!"
    '''
    db = sqlite3.connect(database)
    cur = db.cursor()
    # This part will generate a table that will contain the peptide decoration sequence.
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
    # This is call for SQL that will filter out the unique peptide decoration sequence
    # and put it in the newly created table
    db.execute("insert into seq_deco select seq_decorate from %s"
               " group by seq_decorate having count (*) > 2 order by seq_decorate" %table_name)
    db.commit()
    db.close()
    suess = 'Generation complete! '
    return suess


def purge_numerical_data(database):
    '''
    This is the function that will purge the numerical data. mainly will seperate the number in the decoration sequence
    This function will not be used in this script
    :param database: The database you want to purge the numerical data
    :return: Return a message said "Done!"
    '''
    db = sqlite3.connect(database)
    cur = db.cursor()
    # This will create 2 tables that will be used in the code
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
    # get the whole list of the decoration sequence
    a = list(db.execute("select * from filter_seq_deco"))
    c = []
    b = []
    # parse the decoration in to number
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
    # put the number into a table
    db.executemany("insert into filter_seq_deco2 values (?)", (d))
    db.commit()
    # select the unnique number in the filter seq deco2 table and insert to a new table
    db.execute("insert into filter_seq_deco3 select seq from filter_seq_deco2 group by seq having count (*) > 2 order by seq")
    db.commit()
    db.close()
    sucess = 'Done! '
    return sucess
def break_to_aminoacid(database):
    '''
    This function was used to break the peptide sequence to single aminoacid.
    This function will not be used in this script
    :param database: the database you want to break to aminoacid
    :return:Return a message said "Done!"
    '''
    db = sqlite3.connect(database)
    cur = db.cursor()
    # create two table that will used in the code
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
    # get whole table of peptide sequence
    seq_list = list(db.execute("select * from filter_seq"))
    a = []
    for i in range(len(seq_list)):
        split_seq = seq_list[i][0]
        for i in split_seq:
            a.append(i)
    # put aminoacid sequence into the database
    db.executemany("insert into amino_acid_pre values (?)", (a))
    db.commit()
    db.execute("insert into amino_acid select seq from amino_acid_pre group by seq having count (*) > 2 order by seq")
    db.commit()
    cur.execute('DROP TABLE amino_acid_pre')
    db.close()
    sucess = 'Done! '
    return sucess


def fetch_data(database):
    '''
    This is the function that will seperate the raw data into different aminoacid table base on the aminoacid
    the peptide sequence got.
    This function will not be used in this script
    :param database: This is the database you want to separate the raw data
    :return: Return a message said "Done!"
    '''
    # this will allow the database to preform dirty read
    sqlite3.threadsafety = 2
    db = sqlite3.connect(database, isolation_level=None, check_same_thread=False)
    cur = db.cursor()
    db.execute("PRAGMA read_uncommitted = TRUE")
    db.execute("pragma journal_mode=wal")
    # get basic information for the CPU
    cores = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=cores)
    # get data in the aminoacid list
    seq_list = list(db.execute("select * from amino_acid"))
    sucess = 'Done! '
    long = len(seq_list)
    # this is the multiprocessing part
    for i in range(long):
        sss = seq_list[i][0]
        pool.apply_async(sub_process(sss, db, cur), args=(sss, db, cur))
    pool.close()
    pool.join()
    db.close()
    return sucess

def sub_process(seq_list, db, cur):
    '''
    This is the worker function for the previous function
    :param seq_list: the sequence list
    :param db: the database object
    :param cur: the cursor object
    :return: Return a message said "Done!"
    '''
    numm = seq_list
    table = []
    table.append(numm)
    fin = ''.join(table)
    # create table in the database
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
    # put data in the database
    db.execute("insert into " + fin + " select * from mgfData where seq like  '%" + seq_list + "%'")
    db.commit()
    sucess = 'Done! '
    return sucess


def get_partial(database, database2, size):
    '''
    This is the function will select part data in the raw database and put it into a new database
    (This is essential when the raw database is too large)
    Tnis function will be called in this script
    :param database: The database that contain the raw data
    :param database2: the new database that will be created
    :param size: the size you want to select out
    :return: Return a message said "Done!"
    '''
    db = sqlite3.connect(database)
    cur = db.cursor()
    db2 = sqlite3.connect(database2)
    cur2 = db2.cursor()
    # This will created a table in the new database
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
    # select the whole index of peptide sequence out and put it in a list
    full_list = list(db.execute('select * from filter_seq'))
    # randomly choose size number of peptide sequence and put the corresponding data into the new database
    short_list = random.sample(full_list, size)
    # print(short_list)
    # put the data that select out to the new database
    for i in tqdm.tqdm(range(len(short_list))):
        a = list(db.execute('select * from mgfData where seq = ?', (short_list[i])))
        db2.executemany('insert into random values(?,?,?,?,?,?,?,?,?)', (a))
    # db2.executemany('insert into random select * from  where seq = ?', (short_list))
    db2.commit()
    db2.close()
    db.close()
    success = "Done!"
    return success


def clear_all(database):
    '''
    This is a function will clear the aminoacid table in the database
    This function will not be called in this script
    :param database: The database you want to clear
    :return: no return
    '''
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
    # original database is the database that contain the raw data
    # the out database is the properly generated database that contain less data in it
    original_database = 'test.db'
    out_database = 'test6.db'
    print(generate_seq_list(original_database, table_name='mgfData'))
    print(get_partial(original_database, out_database, size=100))
    print(generate_seq_list(out_database, table_name='random'))
    print(generate_seq_decorate_list(out_database, table_name='random'))
    # purge_numerical_data(database)