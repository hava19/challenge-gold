import binar
import sqlite3
import pandas as pd

service = binar.abusive()

conn = sqlite3.connect('1.db')
cur = conn.cursor()

# Query
table_ori = service.queryCreateTableOri
table_cleansing = service.queryCreateTableClean
table_alay = service.queryCreateTableAlay
table_abusive = service.queryCreateTableAbusive
table_cleansing_text = service.queryCreateTableCleanText

# create table ori tweet
service.createTable(
        conn=conn,
        querySql=table_ori)

# create table cleansing per text 
service.createTable(
        conn=conn,
        querySql=table_cleansing_text
                        )

# create table cleansing
service.createTable(
    conn=conn,
    querySql=table_cleansing)

# create table kamus alay
service.createTable(
    conn=conn,
    querySql=table_alay)

# create table abusive
service.createTable(
    conn=conn,
    querySql=table_abusive)

# put data tweet ori to table original tweet
def putTableOri(connect, file_path):
    
    service.putOriginalTweet(
                        conn =connect,
                        file_csv=file_path,
                        column_ori_csv='Tweet',
                        db_table_name='original_tweet',
                        db_column_name='tweet')
           
# put data abusive.csv to table abusive in db
def putTableAbusive(connect,file_path):
    service.putAbusiveWord(
        conn=connect,
        csv_abusive=file_path,
        column_abusive_csv='ABUSIVE',
        db_table_name='abusive',
        db_column_name='abusive_word')

# put data kamus alay to table kamus_alay in db
def putKamusAlay(connect,file_path):
    service.kamus_alay(
        conn =connect,
        csv_kamus_alay=file_path,
        db_tableName_alay='kamus_alay')

# put data text and cleansing it 
def inputOneText(connect,value):
    # service.putOneText(table=table_name, column=column_name)
    cleanText = service.replaceOneText(
                            conn=connect,
                            column_alay_db=['before','after'],
                            table_alay_db='kamus_alay',
                            table_cleansing_db='cleansing_per_text',
                            column_cleansing_db=['old_tweet','new_tweet'],
                            column_abusive_db='abusive_word',
                            table_abusive_db='abusive',
                            text=value

                            )
    
    dict_response = {
        "id" : cleanText[1],
        "old": value,
        "new" : cleanText[0]
    }

    return dict_response

# remove abusive word
def removeAbusive(connect):
    # remove abusive
    service.removeAbusive(
        conn=connect, 
        table_ori_db='original_tweet', 
        column_ori_db='tweet',
        table_abusive_db='abusive',
        column_abusive_db='abusive_word',
        table_cleansing_db='cleansing_tweet',
        column_cleansing_db='new_tweet')

# replacekamusalay
def replaceKamusAlay(connect):
    # replace kamus alay
    service.replaceKamusAlay(
        conn=connect,
        column_cleansing_db='new_tweet',
        table_cleansing_db='cleansing_tweet',
        column_alay_db=['before','after'],
        table_alay_db='kamus_alay'
                            )

# Get All Data
def getAll(connect):
    alldata = service.getAllData(
                    conn=connect,
                    table_name='cleansing_tweet'
                            )

    return alldata

# Get ID Data
def getIdData(connect,id):
    alldata = service.getIdData(
                    conn=connect,
                    table_name='cleansing_tweet',
                    id_data=id
                            )

    return alldata
