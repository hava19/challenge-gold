import pandas as pd 
import csv
import json

class create_dict(dict): 
    # __init__ function 
    def __init__(self): 
        self = dict() 
          
    # Function to add key:value 
    def add(self, key, value): 
        self[key] = value


class abusive:
    
    
    def __init__(self):
        # query sql to create table original tweet
        self.queryCreateTableOri =  """
                            CREATE TABLE IF NOT EXISTS original_tweet (
                                id integer PRIMARY KEY,
                                tweet text NOT NULL
                                );
                            """
        # query sql to create table cleansing alay per text
        self.queryCreateTableCleanText =  """
                            CREATE TABLE IF NOT EXISTS cleansing_per_text (
                                id integer PRIMARY KEY,
                                old_tweet text,
                                new_tweet text
                                );
                            """
        # query sql to create table cleansing tweet
        self.queryCreateTableClean =  """
                            CREATE TABLE IF NOT EXISTS cleansing_tweet (
                                id integer PRIMARY KEY,
                                new_tweet text
                                );
                            """           
        # query sql to create table abusive word
        self.queryCreateTableAbusive =  """
                            CREATE TABLE IF NOT EXISTS abusive (
                                id integer PRIMARY KEY,
                                abusive_word text NOT NULL
                
                                );
                            """
        # query sql to create table kamus alay
        self.queryCreateTableAlay =  """
                            CREATE TABLE IF NOT EXISTS kamus_alay (
                                id integer PRIMARY KEY
                                );
                            """        


    # function to create query to select all data
    def getAllData(self, conn,table_name):
        
        try:
            querySelectAll = f"""
                            SELECT * FROM {table_name}
                        """

            mydict = create_dict()
            cur = conn.cursor()
            cur.execute(querySelectAll)
            result = cur.fetchall()
            for row in result:
                mydict.add(row[0],({"tweet":row[1]}))

            stud_json = json.dumps(mydict, indent=2, sort_keys=True)

            return stud_json

        except ValueError as e:
            print(e)
            return e
        
    # function to create query to select id data
    def getIdData(self, conn,table_name,id_data):
        
        try:
            querySelectID = f"""
                            SELECT * FROM {table_name} WHERE id = {id_data}
                        """

            mydict = create_dict()
            cur = conn.cursor()
            cur.execute(querySelectID)
            result = cur.fetchall()
            for row in result:
                mydict.add(row[0],({"tweet":row[1]}))

            stud_json = json.dumps(mydict, indent=2, sort_keys=True)

            return stud_json

        except ValueError as e:
            print(e)
            return e


    # function to create table in db
    def createTable(self ,conn, querySql):
        try:
            cur = conn.cursor()
            cur.execute(querySql)
            conn.commit()
            return conn
        except ValueError as e:
            print(e)

    # function to put abusive word into db
    def putAbusiveWord(self,conn,csv_abusive, column_abusive_csv,db_table_name,db_column_name):
        try:
            cur = conn.cursor()
            with open(f'{csv_abusive}','r') as fin: # `with` statement available in 2.5+
                # csv.DictReader uses first line in file for column headings by default
                dr = csv.DictReader(fin) # comma is default delimiter
                to_db = [(i[f'{column_abusive_csv}']) for i in dr]

                df = pd.DataFrame(to_db, columns=[db_column_name])
                df[db_column_name].to_sql(f'{db_table_name}',conn,if_exists='replace', index=False)
            
            conn.commit()
            
            
        except ValueError as error:
            print(error)

    # function to put abusive word into db
    def kamus_alay(self,conn,csv_kamus_alay,db_tableName_alay):

        try:
            cur = conn.cursor()
            filepath = f'{csv_kamus_alay}'
            with open(filepath, mode="rb")as csv:

                kamus_alay = pd.read_csv(csv, header=None, names=['before','after'],encoding='latin-1')
                
                # if_exists can to be replace
                kamus_alay.to_sql(f'{db_tableName_alay}',conn,if_exists='replace', index=False)
                conn.commit()
                
        except ValueError as error:
            print(error)

    # function to put original tweet into db
    def putOriginalTweet(self,conn,file_csv,column_ori_csv,db_table_name,db_column_name):
        try:
            cur = conn.cursor()
            with open(f'{file_csv}','r') as fin: # `with` statement available in 2.5+
                dr = csv.DictReader(fin) # comma is default delimiter
                to_db = [(i[column_ori_csv]) for i in dr]

                # df = pd.DataFrame(to_db, columns=[db_column_name])
                # cur.execute(f"""INSERT OR IGNORE INTO {table_cleansing_db}({column_cleansing_db[0]},{column_cleansing_db[1]}) VALUES (?,?);""",(old_text,new_text_final))

                # df[db_column_name].to_sql(f'{db_table_name}',conn,if_exists='replace', index=False)
                    
            for item in to_db:
                cur.execute(f'INSERT OR REPLACE INTO {db_table_name} ({db_column_name}) VALUES(?)', [item])      
            conn.commit()
            
        except ValueError as error:
            print(error)

    # function remove abusive
    def removeAbusive(self,conn,table_ori_db,column_ori_db,table_abusive_db,column_abusive_db,table_cleansing_db,column_cleansing_db):

        # read column tweet in db
        sql_query_tweet = pd.read_sql('SELECT {column} FROM {table}'.format(column=column_ori_db,table=table_ori_db), conn)

        # read column abusive in db and convert to list
        sql_query_abusive = pd.read_sql('SELECT {column} FROM {table}'.format(column = column_abusive_db, table = table_abusive_db),conn)
        daftar_abusive = sql_query_abusive['{column}'.format(column=column_abusive_db)].tolist()
        
        # lower text of the tweet and clean word user and \n
        sql_query_tweet[column_ori_db] = sql_query_tweet[column_ori_db].str.lower()
        sql_query_tweet[column_ori_db] = sql_query_tweet[column_ori_db].str.replace("user","")
        sql_query_tweet[column_ori_db] = sql_query_tweet[column_ori_db].str.replace(r"\\n","" , regex=True)
        
        # replace abusive tiap baris text tweet
        for abusive in daftar_abusive:
        
            abusive_lower = str(abusive).lower()                
            sql_query_tweet[column_ori_db] = sql_query_tweet[column_ori_db].str.strip().replace(r"\b{}\b".format(abusive_lower),"", regex=True)
        
        try:
            cur = conn.cursor()
            to_db = sql_query_tweet[column_ori_db].tolist()
            for item in to_db:
                cur.execute(f'INSERT OR REPLACE INTO {table_cleansing_db} ({column_cleansing_db}) VALUES(?)', [item])      
            conn.commit()
            # sql_query_tweet[column_ori_db].to_sql(f'{table_cleansing_db}',conn,if_exists='replace', index=False)
            # conn.commit()
                
        except ValueError as error:
            print(error)
        
        print('abusive done !')
    
    # function replace word with kamus_alay
    def replaceKamusAlay(self,conn,column_cleansing_db,table_cleansing_db,column_alay_db,table_alay_db):
        # read column tweet in db
        tweeter = pd.read_sql('SELECT {column} FROM {table}'.format(column=column_cleansing_db,table=table_cleansing_db), conn)
        kamus_alay =pd.read_sql('SELECT {column1},{column2} FROM {table}'.format(
                                                                        column1=column_alay_db[0],
                                                                        column2=column_alay_db[1],
                                                                        table=table_alay_db), conn)

        
        list_tweeter = tweeter['tweet'].str.lower().tolist()
        list_kamus_alay_old = kamus_alay['before'].str.lower().tolist()
        list_kamus_alay_new = kamus_alay['after'].str.lower().tolist()

        
        # tweet = kalimat tweet per baris
        for tweet in list_tweeter:
            index_text = list_tweeter.index(tweet)
            
            # alay = kata alay per baris
            for alay in list_kamus_alay_old:
                
                tweet_split = str(tweet).split(' ')
                
                for z in tweet_split:
                                        
                    index_alay = list_kamus_alay_old.index(alay)

                    if alay == z :
                        
                        index_z = tweet_split.index(z)

                        tweet_split[index_z] = list_kamus_alay_new[index_alay]
                        
                        tweet = ' '.join(tweet_split)
                
            
            list_tweeter[index_text] = tweet
            

        df = pd.DataFrame(list_tweeter, columns=[column_cleansing_db])
        try:
            cur = conn.cursor()
            
            df[column_cleansing_db].to_sql(f'{table_cleansing_db}',conn,if_exists='replace', index=False)
            conn.commit()
                
        except ValueError as error:
            print(error)

        print('alay done !')

    # function replace one text with kamus_alay
    def replaceOneText(self,conn,column_alay_db,table_alay_db,table_cleansing_db,column_cleansing_db,column_abusive_db,table_abusive_db,text):

        old_text = text
        kamus_alay =pd.read_sql('SELECT {column1},{column2} FROM {table}'.format(
                                                                        column1=column_alay_db[0],
                                                                        column2=column_alay_db[1],
                                                                        table=table_alay_db), conn)

        old_alay = kamus_alay['before'].str.lower().tolist()
        new_alay = kamus_alay['after'].str.lower().tolist()

        # read column abusive in db and convert to list
        sql_query_abusive = pd.read_sql('SELECT {column} FROM {table}'.format(column = column_abusive_db, table = table_abusive_db),conn)
        list_abusive = sql_query_abusive['{column}'.format(column=column_abusive_db)].tolist()

        # alay = kata alay per baris
        def replaceAlay(old_alay,new_alay, value):

            list_kamus_alay_old = old_alay
            list_kamus_alay_new = new_alay

            for alay in list_kamus_alay_old:
                
                tweet_split = str(value).split(' ')
                
                for z in tweet_split:
                                        
                    index_alay = list_kamus_alay_old.index(alay)

                    if alay == z :
                        
                        index_z = tweet_split.index(z)

                        tweet_split[index_z] = list_kamus_alay_new[index_alay]
                        
                        value = ' '.join(tweet_split)
            return value
        

        def removeAbusive(daftar_abusive,value):

            for abusive in daftar_abusive:
            
                tweet_split = str(value).split(' ')
            
                for z in tweet_split:
                                    
                    # index_abusive = daftar_abusive.index(abusive)

                    if abusive == z :
                    
                        index_z = tweet_split.index(z)

                        tweet_split[index_z] = ''
                    
                        value = ' '.join(tweet_split)
            return value
        
        
        new_text = removeAbusive(daftar_abusive=list_abusive,value=text)
        new_text_final = replaceAlay(old_alay=old_alay, new_alay=new_alay,value=new_text).strip()

        # df = pd.DataFrame(text, columns=[column_cleansing_db])
        try:
            cur = conn.cursor()
            # df[column_cleansing_db].to_sql(f'{table_cleansing_db}',conn,if_exists='replace', index=False)
            # new_text = ''.join(new_text)
            cur.execute(f"""INSERT OR IGNORE INTO {table_cleansing_db}({column_cleansing_db[0]},{column_cleansing_db[1]}) VALUES (?,?);""",(old_text,new_text_final))
            c = cur.execute(f'SELECT rowid FROM {table_cleansing_db} WHERE {column_cleansing_db[0]}=? AND {column_cleansing_db[1]}=?', (old_text,new_text_final))
            id = c.fetchone()
            print('ini id:',id[0])
            print('ini new :',new_text_final)
            conn.commit()
                
        except ValueError as error:
            print(error)

        print('alay done !')

        return new_text_final,id[0]

    
