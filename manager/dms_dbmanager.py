from dbutils.pooled_db import PooledDB

class DMSDBManager:
    
    '''
    資料庫操作程序
    1. 建立connection pool
    2. 從 connection pool取得connection
    3. 進行資料庫操作
    4. 完成後必須將connection關閉回收至connection pool
    '''

    def __init__(self, logger, **dbconn_config):
        ### Build the connection pool
        self.pool = PooledDB(**dbconn_config)
        self.logger = logger

    def query_database(self, sql):
        conn = self.pool.connection()
        cursor_obj = conn.cursor()
        cursor_obj.execute(sql)
        result = cursor_obj.fetchall()
        result_col = cursor_obj.description
        print("{message : return db query result}")
        self.logger.info("steps in db manager")
        self.logger.info("message : conduct the sql => " + sql)
        conn.close()
        return result, result_col
    
    def query_database_noresult(self, sql):
        conn = self.pool.connection()
        cursor_obj = conn.cursor()
        cursor_obj.execute(sql)
        #self.logger.info("steps in db manager")
        #self.logger.info("message : conduct the sql => " + sql)
        conn.commit()
        conn.close()