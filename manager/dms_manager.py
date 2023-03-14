from .dms_dbmanager import DMSDBManager
import pandas as pd

# 用於操作 dms_dbmanager
class DMSManager:
    def __init__(self, logger, **dbconn_config):
        self.logger = logger
        self.dbmanager = DMSDBManager(self.logger, **dbconn_config)

    # 確認 database 是否有該批號
    def check_db_data(self, table_name, batch_no):
        sql_cmd = "SELECT COUNT(1) FROM {table_name} WHERE BATCH_NO = '{batch_no}'".format(table_name=table_name, batch_no=batch_no)
        table_val, table_col = self.dbmanager.query_database(sql_cmd)
        return table_val

    # 取得 select table 執行結果
    def get_db_table(self, table_name, RESEND_FLAG=None, batch_no=None , creat_dt = None):
        if batch_no and RESEND_FLAG:
            sql_cmd = "SELECT * FROM {table_name} WHERE BATCH_NO = '{batch_no}' AND RESEND_FLAG = 'Y'".format(table_name=table_name, batch_no=batch_no)
        elif batch_no:
            sql_cmd = "SELECT * FROM {table_name} WHERE BATCH_NO = '{batch_no}'".format(table_name=table_name, batch_no=batch_no)
        elif creat_dt:
            sql_cmd = "SELECT *FROM {table_name} WHERE CREATE_DT BETWEEN DATEADD(mm, DATEDIFF(mm,0,getdate()), 0) AND DATEADD(mm, 1, DATEADD(dd, -1, DATEADD(mm, DATEDIFF(mm,0,getdate()), 0)))".format(table_name=table_name)
        else:
            sql_cmd = 'SELECT * FROM {table_name}'.format(table_name=table_name)
        table_val, table_col = self.dbmanager.query_database(sql_cmd)
        df = pd.DataFrame(table_val, columns=[val[0] for val in table_col])
        return df

    # 取得 procedure 執行結果
    def get_db_prc(self, prc_name):
        sql_cmd = 'EXEC {prc_name}'.format(prc_name=prc_name)
        prc_val, prc_col = self.dbmanager.query_database(sql_cmd)
        df = pd.DataFrame(prc_val, columns=[val[0] for val in prc_col])
        return df

    # 填入資料庫對應訂單配送商計算結果
    def fill_distr(self, batch_no, order_no, DISTY_TYPE, REMARK):
        sql_cmd = "UPDATE ORDER_M \
            SET EST_DISTR_TYPE_ID = {DISTY_TYPE}, REMARK = '{REMARK}' \
            WHERE BATCH_NO = '{batch_no}' AND ORDER_NO = '{order_no}'".format(batch_no=batch_no, order_no=order_no, DISTY_TYPE=DISTY_TYPE, REMARK=REMARK)
        self.dbmanager.query_database_noresult(sql_cmd)