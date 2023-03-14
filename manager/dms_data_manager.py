import os
import time
import numpy as np
import pandas as pd
import pymssql
from manager import dms_manager

class DMSDataManager:
    def __init__(self, logger, config, use_cols):
        self.use_cols = use_cols
        self.logger = logger
        self.config = config

    def read_data_from_db(self, batchNo):
        manager = dms_manager.DMSManager(self.logger, **self.config)
        self.ori_money = manager.get_db_prc('DISTR_FEE_INFO')
        self.holiday = manager.get_db_table('HOLIDAY')
        self.ratio = manager.get_db_table('DISTR_M')
        #配送商出貨資訊
        self.DISTR_INFO = manager.get_db_prc('DISTR_INFO')
        #貨主資訊
        self.SHIPPER_INFO = manager.get_db_prc('SHIPPER_INFO')
        self.ORDER_M = manager.get_db_table('ORDER_M', RESEND_FLAG = True, batch_no = batchNo , creat_dt = False)
        con = (self.ORDER_M['SPECIFY_DISTR'] == '') | (pd.isnull(self.ORDER_M.loc[:, 'SPECIFY_DISTR']))
        # 判斷該批號是否全為已指定配送商訂單
        # if len(self.ORDER_M) != 0 and sum(con) == 0:
        #     all_SPECIFY_DISTR = True
        # else:
        #     all_SPECIFY_DISTR = False
        self.ORDER_M = self.ORDER_M.loc[con]
        self.ORDER_M_ratio = manager.get_db_table('ORDER_M', batch_no = None , creat_dt = True)
        self.ORDER_M_ratio = self.ORDER_M_ratio.loc[(self.ORDER_M_ratio['SPECIFY_DISTR'] == '') | (pd.isnull(self.ORDER_M_ratio.loc[:, 'SPECIFY_DISTR']))]
        self.ORDER_D_EST_PACK = manager.get_db_table('ORDER_D_EST_PACK')

        try:
            self.ORDER_D_EST_PACK = self.ORDER_D_EST_PACK[['ORDER_M_ID','PACK_VOLUME']]
            # self.ORDER_D_EST_PACK = self.ORDER_D_EST_PACK.drop_duplicates()
            self.ORDER_M = self.ORDER_M[self.use_cols]
            self.ORDER_M = self.ORDER_M.rename(columns={'ID':'ORDER_M_ID'})
            self.df = pd.merge(self.ORDER_M, self.ORDER_D_EST_PACK, on = 'ORDER_M_ID',how="left")
        except:
            lack_col_indices = [*np.where([not col_name in self.df.columns for col_name in self.use_cols])[0]]
            self.logger.critical('Lack of {} columns'.format([self.use_cols[lack_col_ind] for lack_col_ind in lack_col_indices]))
            os._exit(1)
        self.df['才績級距'] = np.nan
        size_level = [60, 90, 120, 150, 'OTHER']
        size_level1 = [60, 90, 120, 150, 180]
        self.df['才績級距'] = self.df['PACK_VOLUME'].apply(lambda val: size_level[np.where([val <= ele for ele in size_level1])[0][0]])
        box = self.df.groupby('ORDER_NO')
        self.df['箱數'] = 1
        for i in box.groups:
            repeat_val = []
            repeat_val = (self.df[self.df['ORDER_NO']==i]['才績級距']).tolist()
            self.df['才績級距'] = self.df['才績級距'].astype('object')
            self.df['箱數'] = self.df['箱數'].astype('object')
            b = []
            c = []
            for j in list(set(repeat_val)):
                b.append(j)
                c.append(repeat_val.count(j))
            self.df.at[self.df.loc[self.df['ORDER_NO']==i].index[0],'才績級距'] = b
            self.df.at[self.df.loc[self.df['ORDER_NO']==i].index[0],'箱數'] = c
        self.df.drop_duplicates(subset=['ORDER_NO','ORDER_M_ID'], keep='first', inplace=True)
        self.df = self.df.reset_index(drop = True)
        self.df['星期'] = [pd.Timestamp(val).day_name() for val in self.df['CREATE_DT']]

        self.logger.info('Finish loading order information from DB')
        return self.df, self.DISTR_INFO, self.ORDER_M_ratio, self.SHIPPER_INFO, self.ori_money, self.holiday, self.ratio

    def read_data_from_excel(self):
        self.df = pd.read_excel(self.excel_path, '出貨明細')
        try:
            self.df = self.df[self.use_cols]
        except:
            lack_col_indices = [*np.where([not col_name in self.df.columns for col_name in self.use_cols])[0]]
            self.logger.critical('Lack of {} columns'.format([self.use_cols[lack_col_ind] for lack_col_ind in lack_col_indices]))
            os._exit(1)

        self.df['長度'] = np.nan
        self.df['才績級距'] = np.nan

        ### 確認有無缺少欄位，並同時將這些所需欄位中有缺失值的資料進行刪除
        comb_col = ['耗材尺寸', '耗材材積']
        if np.all([val in self.df.columns for val in comb_col]):
            # 去掉同時沒有comb_col的資料
            # print('有缺失資料列數indices如下:{}'.format(~np.all(self.df.isna().loc[:, comb_col])))
            self.df = self.df[~np.all(self.df.isna().loc[:, comb_col], axis=1)]
            # 去掉comb_col外的其他欄位
            other_col = list(self.df.columns[~np.any([val == self.df.columns for val in comb_col], axis=0)])[:len(self.use_cols)-len(comb_col)]
            self.df = self.df.loc[~np.any(self.df[other_col].isna(), axis=1)]
        else:
            #print('缺少{}欄位'.format(comb_col))
            pass
        self.df.reset_index(inplace=True)

        ### 透過耗材尺寸補長度，最後用長度補才績級距
        non_nan_ind = ~self.df['耗材尺寸'].isna()
        self.df['長度'].loc[non_nan_ind] = self.df['耗材尺寸'].loc[non_nan_ind].apply(lambda val: sum(list(map(float, val.split('*')))))
        size_level = [60, 90, 120, 150, 180]
        # 如果該長度資料有缺失，則該列有缺失資料透過耗材才積補
        nan_length_ind = self.df['長度'].isna()
        len_level = [0, 0.3, 1, 2, 3]
        self.df.loc[nan_length_ind, '長度'] = self.df.loc[nan_length_ind, '耗材材積'].apply(lambda val: size_level[np.where([val >= ele for ele in len_level])[0][-1]])
        self.df['才績級距'] = self.df['長度'].apply(lambda val: size_level[np.where([val <= ele for ele in size_level])[0][0]])
        
        ### 計算星期幾
        self.df['星期'] = [pd.Timestamp(val).day_name() for val in self.df['訂單日期']]
        self.logger.info('Finish loading order information')

        return self.df

    def generate_result(self, df):
        name = time.strftime("%y_%m_%d_%H%M%S")
        df.to_csv('./' + name + 'result.csv', encoding='utf-8-sig')
        self.logger.info('Generate csv file in the folder')
