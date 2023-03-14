import numpy as np
import pandas as pd
from datetime import datetime,timedelta
import time
from tqdm import tqdm

class rule:
    def __init__(self ,df ,DISTR_INFO,ORDER_M_ratio, SHIPPER_INFO, ori_money, holiday, ratio, islands, logger):
        self.df = df
        self.holiday = holiday
        self.DISTR_INFO = DISTR_INFO
        self.ORDER_M_ratio = ORDER_M_ratio
        self.SHIPPER_INFO = SHIPPER_INFO
        self.ori_money = ori_money
        self.ratio = ratio
        self.islands = islands
        self.logger = logger
        ## 將星期轉換為數字用
        self.week = {1:['Monday'], 
                    2:['Tuesday'],
                    3:['Wednesday'],
                    4:['Thursday'],
                    5:['Friday'],
                    6:['Saturday'],
                    7:['Sunday']}

    ### 平移日期程式，用於出貨為隔日或周一的日期平移    
    def next_weekday(self,weekday, d = datetime.now()):
        delta = weekday-d.isoweekday()
        if delta<=0:
            delta+=7
        return d + timedelta(delta)
    
    ## 此函數主要任務是找到符合條件的配送商
    def build_limitation(self):
        ## 如果抓出來沒有資料則不執行程式
        if len(self.df) == 0:
            self.logger.warning('There is no computable data in this batch')
            return
        self.logger.info('Start to shipper condition judgment')
        # 填上配送時效及歸類，利用貨主資訊將此筆訂單對應之條件填上dataframe
        self.df['配送時效'] = np.nan
        self.df['貨主歸類'] = np.nan
        self.df['賠償上限'] = np.nan
        self.df['出車星期'] = np.nan
        self.df['符合條件配送商'] = np.nan
        self.df['符合條件配送商'] = self.df['符合條件配送商'].astype('object')
        try:
            for ind_name in self.SHIPPER_INFO['SHIPPER_CODE'].unique():
                dls = ''.join(self.SHIPPER_INFO[self.SHIPPER_INFO['SHIPPER_CODE']==ind_name]['DISTR_LIMITATION_CODE'].unique())
                if self.SHIPPER_INFO[self.SHIPPER_INFO['SHIPPER_CODE']==ind_name]['SPECIAL_REQ'].unique().tolist() != [None]:
                    dls = dls+','+(''.join(self.SHIPPER_INFO[self.SHIPPER_INFO['SHIPPER_CODE']==ind_name]['SPECIAL_REQ'].unique()))
                self.df.loc[self.df['SHIPPER_CODE'] == ind_name, '配送時效'] = self.df.loc[self.df['SHIPPER_CODE'] == ind_name, '配送時效'].apply(lambda val: dls.split(','))
                self.df.loc[self.df['SHIPPER_CODE'] == ind_name, '貨主歸類'] = self.df.loc[self.df['SHIPPER_CODE'] == ind_name, '貨主歸類'].apply(lambda val: self.SHIPPER_INFO[self.SHIPPER_INFO['SHIPPER_CODE']==ind_name]['SHIPPER_TYPE_CODE'].unique().tolist()[0])
                self.df.loc[self.df['SHIPPER_CODE'] == ind_name, '賠償上限'] = self.df.loc[self.df['SHIPPER_CODE'] == ind_name, '賠償上限'].apply(lambda val: self.SHIPPER_INFO[self.SHIPPER_INFO['SHIPPER_CODE']==ind_name]['COMPENSATION'].unique().tolist()[0])
        except:
            pass
        # 處理時間格式
        self.SHIPPER_INFO['ORDER_WEEK_S'] = self.SHIPPER_INFO['ORDER_WEEK_S'].astype(float)
        self.SHIPPER_INFO['ORDER_WEEK_E'].fillna(value=self.SHIPPER_INFO['ORDER_WEEK_S'],inplace=True)
        self.DISTR_INFO['COLLECT_WEEK_S'] = self.DISTR_INFO['COLLECT_WEEK_S'].astype(float)
        self.DISTR_INFO['COLLECT_WEEK_E'].fillna(value=self.DISTR_INFO['COLLECT_WEEK_S'],inplace=True)      
        for i in range(len(self.SHIPPER_INFO)):
            self.SHIPPER_INFO.loc[i,'ORDER_TIME_S'] = datetime.strftime(datetime.strptime(str(self.SHIPPER_INFO.loc[i,'ORDER_TIME_S']), "%H:%M"),"%H:%M")
            self.SHIPPER_INFO.loc[i,'ORDER_TIME_E'] = datetime.strftime(datetime.strptime(str(self.SHIPPER_INFO.loc[i,'ORDER_TIME_E']), "%H:%M"),"%H:%M")

        ### 計算下單日期，決定對應的出車時間
        self.df['預估出車時間'] = np.nan
        self.df['預估出車時間(日)'] = np.nan
        self.df['訂單下單星期'] = self.df['星期'].apply(lambda val: list(self.week.keys())[np.where([val in cor_val for cor_val in self.week.values()])[0][0]])
        self.SHIPPER_INFO['ORDER_TIME_E'].replace(datetime.strftime(datetime.strptime('00:00', "%H:%M"),"%H:%M"), datetime.strftime(datetime.strptime('23:59', "%H:%M"),"%H:%M"), inplace=True)
        SHIPPER_CODE_1 = self.SHIPPER_INFO.groupby('SHIPPER_CODE')
        for ind in range(len(self.df)):
            ## 如果沒有材積或是貨主資訊無法對應，會在此加上remark並且後面程式不會去篩選有此remark的訂單
            if str(self.df.loc[ind,'才績級距']) == 'nan':
                self.df.loc[ind,'REMARK'] = 'There is no cuft volume for this order'
                self.df.at[ind,'符合條件配送商'] = []
                self.df.loc[ind,'EST_DISTR_TYPE_ID'] = ''
                continue
            if str(self.df.loc[ind,'貨主歸類']) == 'nan':
                self.df.loc[ind,'REMARK'] = 'Can not find the corresponding shipper'
                self.df.loc[ind,'配送時效'] = ['nan']
                try:
                    self.df.loc[ind, '出車時間'] = datetime.strptime(str(self.df.loc[ind]['CREATE_DT']), "%Y-%m-%d %H:%M:%S.%f").strftime("%Y/%m/%d")
                except:
                    self.df.loc[ind, '出車時間'] = datetime.strptime(str(self.df.loc[ind]['CREATE_DT']), "%Y-%m-%d %H:%M:%S").strftime("%Y/%m/%d")                   
                self.df.at[ind,'符合條件配送商'] = []
                self.df.loc[ind,'EST_DISTR_TYPE_ID'] = ''
                continue
            ## 填上出車時間及預估出車時間、星期等
            try:
                self.df.loc[ind, '訂單下單時段'] = datetime.strftime(datetime.strptime(str(self.df.loc[ind,'CREATE_DT']),"%Y-%m-%d %H:%M:%S"),"%H:%M")
            except:
                self.df.loc[ind, '訂單下單時段'] = datetime.strftime(datetime.strptime(str(self.df.loc[ind,'CREATE_DT']),"%Y-%m-%d %H:%M:%S.%f"),"%H:%M")

            con_s = SHIPPER_CODE_1.get_group(self.df.loc[ind, 'SHIPPER_CODE'])
            con_ss = con_s.loc[(self.df.loc[ind, '訂單下單時段']>=con_s['ORDER_TIME_S']) & (self.df.loc[ind, '訂單下單時段']<con_s['ORDER_TIME_E']) & (self.df.loc[ind, '訂單下單星期']>=con_s['ORDER_WEEK_S']) & (self.df.loc[ind, '訂單下單星期']<=con_s['ORDER_WEEK_E'])]
            self.df.loc[ind, '預估出車時間'] = con_ss['EST_DISPATCH_TIME'].values[0]
            self.df.loc[ind, '預估出車時間(日)'] = con_ss['EST_DISPATCH_DAY'].values[0]
            if self.df.loc[ind, '預估出車時間(日)'] == 8:
                try:
                    self.df.loc[ind, '出車時間'] = (datetime.strptime(str(self.df.loc[ind]['CREATE_DT']), "%Y-%m-%d %H:%M:%S.%f") + timedelta(days=1)).strftime("%Y/%m/%d")
                except:
                    self.df.loc[ind, '出車時間'] = (datetime.strptime(str(self.df.loc[ind]['CREATE_DT']), "%Y-%m-%d %H:%M:%S") + timedelta(days=1)).strftime("%Y/%m/%d")

            elif self.df.loc[ind, '預估出車時間(日)'] == 0:
                try:
                    self.df.loc[ind, '出車時間'] = datetime.strptime(str(self.df.loc[ind]['CREATE_DT']), "%Y-%m-%d %H:%M:%S.%f").strftime("%Y/%m/%d")
                except:
                    self.df.loc[ind, '出車時間'] = datetime.strptime(str(self.df.loc[ind]['CREATE_DT']), "%Y-%m-%d %H:%M:%S").strftime("%Y/%m/%d")
                   
            else:
                try:
                    self.df.loc[ind, '出車時間'] = self.next_weekday(weekday=self.df.loc[ind, '預估出車時間(日)'],d = datetime.strptime(str(self.df.loc[ind]['CREATE_DT']), "%Y-%m-%d %H:%M:%S.%f")).strftime("%Y/%m/%d")
                except:
                    self.df.loc[ind, '出車時間'] = self.next_weekday(weekday=self.df.loc[ind, '預估出車時間(日)'],d = datetime.strptime(str(self.df.loc[ind]['CREATE_DT']), "%Y-%m-%d %H:%M:%S")).strftime("%Y/%m/%d")

         
        self.df['出車星期'] = [pd.Timestamp(val).day_name() for val in self.df['出車時間']]
        self.df['出車星期'] = self.df['出車星期'].apply(lambda val: list(self.week.keys())[np.where([val in cor_val for cor_val in self.week.values()])[0][0]])
        self.logger.info('Complete the basic field data capture')


        # 首先透過配送時效判斷訂單為當日配或隔日配
        self.df['當日隔日'] = self.df['配送時效'].apply(lambda val_list: \
            '當日' if np.any([((val[-7:] == 'SameDay') or (val[-7:] == 'Express')) for val in val_list]) \
                else '隔日')
        
        ### 假日表邏輯
        #################################
        self.logger.info('Start Holiday Table Calculation')
        ## 判斷此日期是否為國定假日（在假日表中）
        ## 在假日表中的日期將轉換出車星期為週日
        try:
            ff = self.holiday.groupby('HOLIDAY_TYPE')
            holiday_list = []
            for i in list(ff.get_group(2).index):
                timeString = str(ff.get_group(2)['HOLIDAY_DT'][i]) # 輸入原始字串
                struct_time = time.strptime(timeString, "%Y-%m-%d") # 轉成時間元組
                new_timeString = time.strftime("%Y/%m/%d", struct_time)
                holiday_list.append(new_timeString)
                
            for i in list(ff.get_group(3).index):
                timeString = str(ff.get_group(3)['HOLIDAY_DT'][i]) # 輸入原始字串
                struct_time = time.strptime(timeString, "%Y-%m-%d") # 轉成時間元組
                new_timeString = time.strftime("%Y/%m/%d", struct_time)
                holiday_list.append(new_timeString)
        except:
            self.logger.error('There is a mistake in the format of the holiday form input')
            pass
        #################################
        ## 開始找出符合條件之配送商
        self.DISTR_INFO['DISTR_COND_CODE'].fillna(value='haha',inplace=True)
        self.logger.info('Start filter out eligible distributors')
        ## 第一次迴圈先判斷SA/COD 及當日需出貨之訂單
        # self.DISTR_INFO['COLLECT_WEEK_S'] = self.DISTR_INFO['COLLECT_WEEK_S'].astype(str)
        self.df['出車星期'] = self.df['出車星期'].astype(float)
        for i in range(len(self.df)):
            zip_con = False
            if self.df.loc[i,'REMARK'] == 'Can not find the corresponding shipper':
                continue
            list_today = []
            list_hol = []
            if self.df.loc[i]['出車時間'] in holiday_list:
                self.df.loc[i]['出車星期'] = 7
            list_d = []
            list_sacod = []
            if self.df['IS_SA'][i] == 'Y' and self.df['COD'][i] != 0:
                sa_con = self.DISTR_INFO[self.DISTR_INFO['DISTR_COND_CODE'].str.contains('sa','cod')]
                sa_con = sa_con.reset_index(drop = True)
                try:
                    for j in range(len(sa_con)):
                        if str(int(self.df.loc[i, 'RECEIVE_ZIP_CODE'])) in sa_con.loc[j, 'ZIP_CODE'] and self.df.loc[i, '貨主歸類'] in sa_con.loc[j, 'SHIPPER_TYPE_CODE']:
                            if sa_con.loc[j, 'DISTR_TYPE_ID'] not in list_sacod:
                                list_sacod.append(sa_con.loc[j, 'DISTR_TYPE_ID'])
                            continue
                except:
                    pass

            elif self.df['IS_SA'][i] == 'Y':
                sa_con = self.DISTR_INFO[self.DISTR_INFO['DISTR_COND_CODE'].str.contains('sa')]
                sa_con = sa_con.reset_index(drop = True)
                try:
                    for j in range(len(sa_con)):
                        if str(int(self.df.loc[i, 'RECEIVE_ZIP_CODE'])) in sa_con.loc[j, 'ZIP_CODE'] and self.df.loc[i, '貨主歸類'] in sa_con.loc[j, 'SHIPPER_TYPE_CODE']:
                            if sa_con.loc[j, 'DISTR_TYPE_ID'] not in list_sacod:
                                list_sacod.append(sa_con.loc[j, 'DISTR_TYPE_ID'])
                            continue
                except:
                    pass

            elif self.df['COD'][i] != 0:
                cod_con = self.DISTR_INFO[self.DISTR_INFO['DISTR_COND_CODE'].str.contains('cod')]
                cod_con = cod_con.reset_index(drop = True)
                try:
                    for j in range(len(cod_con)):
                        if str(int(self.df.loc[i, 'RECEIVE_ZIP_CODE'])) in cod_con.loc[j, 'ZIP_CODE'] and self.df.loc[i, '貨主歸類'] in cod_con.loc[j, 'SHIPPER_TYPE_CODE']:
                            if cod_con.loc[j, 'DISTR_TYPE_ID'] not in list_sacod:
                                list_sacod.append(cod_con.loc[j, 'DISTR_TYPE_ID'])
                            continue
                except:
                    pass
            else:
                if self.df.loc[i, '當日隔日'] == '當日':
                    # dis_sa =self.DISTR_INFO[self.DISTR_INFO['DISTR_COND_CODE'].isnull()==True]       
                    con_d = self.DISTR_INFO.loc[(self.df.loc[i, '出車星期']>=self.DISTR_INFO['COLLECT_WEEK_S']) & (self.df.loc[i, '出車星期']<=self.DISTR_INFO['COLLECT_WEEK_E']) & (self.df.loc[i, '預估出車時間']<=self.DISTR_INFO['DISPATCH_TIME']) & (self.df.loc[i, '賠償上限']<=self.DISTR_INFO['COMPENSATION'])]
                    con_d = con_d.reset_index(drop=True)
                    
                    for j in range(len(con_d)):
                        if str(int(self.df.loc[i, 'RECEIVE_ZIP_CODE'])) in con_d.loc[j, 'ZIP_CODE'] and self.df.loc[i, '貨主歸類'] in con_d.loc[j, 'SHIPPER_TYPE_CODE'] and con_d.loc[j, 'DISTR_LIMITATION_CODE'] in self.df.loc[i, '配送時效']:
                            if self.df.loc[i, '當日隔日'] == '當日' and (con_d.loc[j, 'DISTR_LIMITATION_CODE'][-7:] == 'SameDay' or con_d.loc[j, 'DISTR_LIMITATION_CODE'][-7:] == 'Express'):
                                if con_d.loc[j, 'DISTR_TYPE_ID'] not in list_today:
                                    list_today.append(con_d.loc[j, 'DISTR_TYPE_ID'])
                    forbit = []
                    for x in self.df.loc[i]['才績級距']:
                        for j in range(len(list_today)):
                            if (self.ori_money[self.ori_money['DISTR_TYPE_ID'] == list_today[j]]['OUTLYINGIS']).values[0] is None and self.df.loc[i, 'RECEIVE_ZIP_CODE'] in self.islands:
                                forbit.append(list_today[j])
                            elif (self.ori_money[self.ori_money['DISTR_TYPE_ID'] == list_today[j]][str(x)]).values[0] is None:
                                forbit.append(list_today[j])
                        list_today = list(set(list_today)-set(forbit))
                    if  list_today == []:
                        ## 如果需要當日配但沒有任何符合條件，則變成當日隔配(當日配貨件用當日的隔日配出貨或是用隔日當日配出貨)
                        self.df.loc[i, '當日隔日'] ='當日隔配'                     
                ## 非當日配需求這裡直接判斷隔日配邏輯
                else:
                    con_n = self.DISTR_INFO.loc[(self.df.loc[i, '賠償上限']<=self.DISTR_INFO['COMPENSATION'])]
                    con_n = con_n.reset_index(drop=True)
                    zip_con = True
                    for j in range(len(con_n)):  
                        if str(int(self.df.loc[i, 'RECEIVE_ZIP_CODE'])) in con_n['ZIP_CODE'][j]:
                            zip_con = False
                        if str(int(self.df.loc[i, 'RECEIVE_ZIP_CODE'])) in con_n.loc[j, 'ZIP_CODE'] and self.df.loc[i, '貨主歸類'] in con_n.loc[j, 'SHIPPER_TYPE_CODE'] and con_n.loc[j, 'DISTR_LIMITATION_CODE'] in self.df.loc[i, '配送時效']:      
                            if self.df.loc[i, '出車星期'] == 5 and 'satDelivery' in self.df.loc[i, '配送時效']:
                                ## 如果此訂單在週五出貨且需要週六送達，他的配送商就需要可以當日或隔日配達（list_hol）
                                if con_n.loc[j,'ARRIVAL_DAY']!= 0 and con_n.loc[j,'ARRIVAL_DAY']!= 8:
                                    if con_n.loc[j, 'DISTR_TYPE_ID'] not in list_d:
                                        list_d.append(con_n.loc[j, 'DISTR_TYPE_ID'])
                                else:
                                    if con_n.loc[j, 'DISTR_TYPE_ID'] not in list_hol:
                                        list_hol.append(con_n.loc[j, 'DISTR_TYPE_ID'])
                            elif self.df.loc[i, '出車星期'] == 6 and 'sunDelivery' in self.df.loc[i, '配送時效']:
                                if con_n.loc[j,'ARRIVAL_DAY']!= 0 and con_n.loc[j,'ARRIVAL_DAY']!= 8:
                                    if con_n.loc[j, 'DISTR_TYPE_ID'] not in list_d:
                                        list_d.append(con_n.loc[j, 'DISTR_TYPE_ID'])
                                else:
                                    if con_n.loc[j, 'DISTR_TYPE_ID'] not in list_hol:
                                        list_hol.append(con_n.loc[j, 'DISTR_TYPE_ID'])
                            else:
                                if con_n.loc[j, 'DISTR_TYPE_ID'] not in list_d:
                                    list_d.append(con_n.loc[j, 'DISTR_TYPE_ID'])
            if list_today !=[]:
                ## 刪掉只for sa的配送商
                list_today = list(set(list_today) - set(list(set(self.DISTR_INFO[self.DISTR_INFO['DISTR_COND_CODE'].str.contains('sa')]['DISTR_TYPE_ID'].tolist()))))                
                self.df.at[i,'符合條件配送商'] = list_today
            elif list_sacod != []:
                self.df.at[i,'符合條件配送商'] = list_sacod
            elif list_hol !=[]:
                list_hol = list(set(list_hol) - set(list(set(self.DISTR_INFO[self.DISTR_INFO['DISTR_COND_CODE'].str.contains('sa')]['DISTR_TYPE_ID'].tolist()))))     
                self.df.at[i,'符合條件配送商'] = list_hol
            else:
                list_d = list(set(list_d) - set(list(set(self.DISTR_INFO[self.DISTR_INFO['DISTR_COND_CODE'].str.contains('sa')]['DISTR_TYPE_ID'].tolist()))))     
                self.df.at[i,'符合條件配送商'] = list_d

            if list_sacod == [] and self.df['IS_SA'][i] == 'Y':
                self.df.loc[i,'REMARK'] = 'SA shipping required but no eligible shippers'
            elif list_sacod == [] and self.df['COD'][i] != 0:
                self.df.loc[i,'REMARK'] = 'COD shipping required but no eligible shippers'
            elif list_d == [] and list_today ==[] and list_sacod == [] and self.df.loc[i, '當日隔日'] != '當日隔配':
                if zip_con == True:
                    self.df.loc[i,'REMARK'] = 'There are no available distributors for this postal code'
                else:
                    self.df.loc[i,'REMARK'] = 'Unable to find eligible distributors within limits'

        ## 第二次迴圈判斷前面無法成功計算需當日配送的訂單
        for i in range(len(self.df)):
            if self.df.loc[i,'REMARK'] == 'Can not find the corresponding shipper':
                continue
            list_today = []
            if self.df.loc[i, '當日隔日'] == '當日隔配':
                con_d = self.DISTR_INFO.loc[(self.df.loc[i, '出車星期']>=self.DISTR_INFO['COLLECT_WEEK_S']) & (self.df.loc[i, '出車星期']<=self.DISTR_INFO['COLLECT_WEEK_E']) & (self.df.loc[i, '預估出車時間']<=self.DISTR_INFO['DISPATCH_TIME']) & (self.df.loc[i, '賠償上限']<=self.DISTR_INFO['COMPENSATION'])]
                con_d = con_d.reset_index(drop=True)
                zip_con = True
                for j in range(len(con_d)):
                    if str(int(self.df.loc[i, 'RECEIVE_ZIP_CODE'])) in con_d['ZIP_CODE'][j]:
                        zip_con = False                    
                    if str(int(self.df.loc[i, 'RECEIVE_ZIP_CODE'])) in con_d.loc[j, 'ZIP_CODE'] and self.df.loc[i, '貨主歸類'] in con_d.loc[j, 'SHIPPER_TYPE_CODE']:
                        if con_d.loc[j, 'DISTR_TYPE_ID'] not in list_today:
                            list_today.append(con_d.loc[j, 'DISTR_TYPE_ID'])
                if list_today ==[]:
                    self.df.loc[i, '出車星期'] = self.df.loc[i, '出車星期']+1
                if self.df.loc[i, '出車星期'] == 8:
                    self.df.loc[i, '出車星期'] = 1
                con_d = self.DISTR_INFO.loc[(self.df.loc[i, '出車星期']>=self.DISTR_INFO['COLLECT_WEEK_S']) & (self.df.loc[i, '出車星期']<=self.DISTR_INFO['COLLECT_WEEK_E']) & (self.df.loc[i, '賠償上限']<=self.DISTR_INFO['COMPENSATION'])]
                con_d = con_d.reset_index(drop=True)
                for j in range(len(con_d)):
                    if str(int(self.df.loc[i, 'RECEIVE_ZIP_CODE'])) in con_d['ZIP_CODE'][j]:
                        zip_con = False 
                    if str(int(self.df.loc[i, 'RECEIVE_ZIP_CODE'])) in con_d.loc[j, 'ZIP_CODE'] and self.df.loc[i, '貨主歸類'] in con_d.loc[j, 'SHIPPER_TYPE_CODE'] and con_d.loc[j, 'DISTR_LIMITATION_CODE'] in self.df.loc[i, '配送時效']:
                        if (con_d.loc[j, 'DISTR_LIMITATION_CODE'][-7:] == 'SameDay' or con_d.loc[j, 'DISTR_LIMITATION_CODE'][-7:] == 'Express'):
                            if con_d.loc[j, 'DISTR_TYPE_ID'] not in list_today:
                                list_today.append(con_d.loc[j, 'DISTR_TYPE_ID'])
                forbit = []
                for x in self.df.loc[i]['才績級距']:
                    for j in range(len(list_today)):
                        if (self.ori_money[self.ori_money['DISTR_TYPE_ID'] == list_today[j]]['OUTLYINGIS']).values[0] is None and self.df.loc[i, 'RECEIVE_ZIP_CODE'] in self.islands:
                            forbit.append(list_today[j])
                        elif (self.ori_money[self.ori_money['DISTR_TYPE_ID'] == list_today[j]][str(x)]).values[0] is None:
                            forbit.append(list_today[j])
                    list_today = list(set(list_today)-set(forbit))
                if list_today !=[]:
                    list_today = list(set(list_today) - set(list(set(self.DISTR_INFO[self.DISTR_INFO['DISTR_COND_CODE'].str.contains('sa')]['DISTR_TYPE_ID'].tolist()))))     
                    self.df.at[i,'符合條件配送商'] = list_today
                elif list_today ==[]:
                    con_d = self.DISTR_INFO.loc[(self.df.loc[i, '出車星期']>=self.DISTR_INFO['COLLECT_WEEK_S']) & (self.df.loc[i, '出車星期']<=self.DISTR_INFO['COLLECT_WEEK_E']) & (self.df.loc[i, '賠償上限']<=self.DISTR_INFO['COMPENSATION'])]
                    con_d = con_d.reset_index(drop=True)
                    for j in range(len(con_d)):
                        if str(int(self.df.loc[i, 'RECEIVE_ZIP_CODE'])) in con_d.loc[j, 'ZIP_CODE'] and self.df.loc[i, '貨主歸類'] in con_d.loc[j, 'SHIPPER_TYPE_CODE'] and con_d.loc[j, 'DISTR_LIMITATION_CODE'] in self.df.loc[i, '配送時效']:
                            if con_d.loc[j, 'DISTR_TYPE_ID'] not in list_today:
                                list_today.append(con_d.loc[j, 'DISTR_TYPE_ID'])
                if list_today !=[]:
                    list_today = list(set(list_today) - set(list(set(self.DISTR_INFO[self.DISTR_INFO['DISTR_COND_CODE'].str.contains('sa')]['DISTR_TYPE_ID'].tolist()))))     
                    self.df.at[i,'符合條件配送商'] = list_today
                elif list_today ==[]:
                    if zip_con:
                        self.df.loc[i,'REMARK'] = 'There are no available distributors for this postal code'
                    else:
                        self.df.loc[i,'REMARK'] = 'Unable to find eligible distributors within limits'

            
    ### 利用材積換算運價，選出最便宜之配送商
    def find_cheapest_carrier(self):
        if len(self.df) == 0:
            return
        self.logger.info('Choose a distributor by the lowest cost')
        for i in range(len(self.df)):
            self.df.at[i,'符合條件配送商'] = list(set(self.df.loc[i,'符合條件配送商'] ) & set(self.ori_money['DISTR_TYPE_ID'].unique().tolist()))
            if self.df.loc[i,'REMARK'] == 'There is no cuft volume for this order':
                continue
            for x in self.df.loc[i]['才績級距']:
                forbit = []
                forbit_con = False
                for j in range(len(self.df.loc[i]['符合條件配送商'])):
                    if (self.ori_money[self.ori_money['DISTR_TYPE_ID'] == self.df.loc[i]['符合條件配送商'][j]]['OUTLYINGIS']).values[0] is None and self.df.loc[i, 'RECEIVE_ZIP_CODE'] in self.islands:
                        forbit.append(self.df.loc[i]['符合條件配送商'][j])
                        forbit_con = True
                    elif (self.ori_money[self.ori_money['DISTR_TYPE_ID'] == self.df.loc[i]['符合條件配送商'][j]][str(x)]).values[0] is None:
                        forbit.append(self.df.loc[i]['符合條件配送商'][j])
                        forbit_con = True
                self.df.at[i,'符合條件配送商'] = list(set(self.df.loc[i,'符合條件配送商'])-set(forbit))
            if self.df.loc[i,'REMARK'] == 'Can not find the corresponding shipper':
                continue
            if len(self.df.loc[i]['符合條件配送商']) == 1:
                self.df['EST_DISTR_TYPE_ID'][i] = self.df['符合條件配送商'][i][0]
            elif len(self.df.loc[i]['符合條件配送商']) == 0:
                if forbit_con:
                    self.df.loc[i,'REMARK'] = 'There is no distributor for this cuft'
                self.df['EST_DISTR_TYPE_ID'][i] = ''
            elif self.df.loc[i, 'RECEIVE_ZIP_CODE'] in self.islands:
                initial = 9999999
                for j in self.df['符合條件配送商'][i]:
                    volume = self.ori_money[self.ori_money['DISTR_TYPE_ID'] == j]['OUTLYINGIS']
                    try:
                        volume = volume.astype(float)
                        volume = round(float(volume.values),2)
                        if volume < initial:
                            initial = volume
                            fin_carrier = j
                    except:
                        pass
                self.df['EST_DISTR_TYPE_ID'][i] = fin_carrier
        
            else:
                initial = 9999999
                for j in self.df['符合條件配送商'][i]: 
                    volume = 0
                    for x in self.df.loc[i]['才績級距']:
                        volume = self.ori_money[self.ori_money['DISTR_TYPE_ID'] == j][str(x)] + volume
                    
                    volume = volume.astype(float)
                    volume = round(float(volume.values),2)
                    if volume < initial:
                        initial = volume
                        fin_carrier = j
                    
                self.df['EST_DISTR_TYPE_ID'][i] = fin_carrier
        if all(self.df['EST_DISTR_TYPE_ID']) == False:
            self.logger.warning('There are {} distributors with null values'.format(self.df['EST_DISTR_TYPE_ID'].value_counts()['']))
            self.logger.warning(('The order number is as follows: {}'.format(list(self.df[self.df['EST_DISTR_TYPE_ID'] == '']['ORDER_NO']))))
        
    def adjustment_ratio(self):
        if len(self.df) == 0:
            return
        self.logger.info('Start adjusting the ratio of carriers')
        self.DIS_TYPE_GROUP = self.DISTR_INFO.groupby('DISTR_ID')
        self.DIS_TYPE = {}
        self.ORDER_M_ratio = self.ORDER_M_ratio[self.ORDER_M_ratio['EST_DISTR_TYPE_ID'].isnull()==False]
        df_ratio = self.df.append(self.ORDER_M_ratio)
        df_ratio = df_ratio.reset_index(drop = True)
        for i in list(self.DIS_TYPE_GROUP.size().keys()):
            self.DIS_TYPE[i] = list(set(list(self.DIS_TYPE_GROUP.get_group(i)['DISTR_TYPE_ID'])))
        group = self.ratio.groupby('ID')
        self.df1 = pd.DataFrame(columns=['DISTR_ID','count','ASSIGN_LIMIT_LOWER','ASSIGN_LIMIT_UPPER','Adjustment','Adjustment_U'])
        for i in list(group.size().keys()):
            count_dis = 0
            try:
                for j in range(len(self.DIS_TYPE[i])):
                    count_dis += df_ratio[df_ratio['EST_DISTR_TYPE_ID']==self.DIS_TYPE[i][j]]['EST_DISTR_TYPE_ID'].count()
                self.df1.loc[len(self.df1.index)] = [i,count_dis,((float(group.get_group(i)['ASSIGN_LIMIT_LOWER'])/100)*len(df_ratio)),float((group.get_group(i)['ASSIGN_LIMIT_UPPER'])/100*len(df_ratio)),None,None]
            except:
                pass
        group3 = self.df1.groupby('DISTR_ID')
        for i in list(group3.size().keys()):
            self.df1.loc[float(self.df1.loc[self.df1['DISTR_ID']==i].index.values),'Adjustment_U'] = float(group3.get_group(i)['ASSIGN_LIMIT_UPPER']) - float(group3.get_group(i)['count'])
            self.df1.loc[float(self.df1.loc[self.df1['DISTR_ID']==i].index.values),'Adjustment'] = float(group3.get_group(i)['ASSIGN_LIMIT_LOWER']) - float(group3.get_group(i)['count'])
        rankk = pd.DataFrame(columns=['DISTR_ID','rank'])
        for j in self.DIS_TYPE:
            count = 0
            summ = 0
            for i in range(len(self.ori_money)):
                if self.ori_money['DISTR_TYPE_ID'][i] in self.DIS_TYPE[j]:
                    count+=1
                    summ+= self.ori_money['60'][i]
            try:
                rankk.loc[len(rankk.index)] = [j,summ/count]
            except:
                pass
        self.df1 = pd.merge(self.df1,rankk, on = 'DISTR_ID',how="left")
        self.df1 = self.df1.sort_values(by='rank',ascending = False)
        self.df1 = self.df1.reset_index(drop = True)
        self.df1['Adjustment_U'] = round(self.df1['Adjustment_U'],2)
        self.df1['Adjustment'] = round(self.df1['Adjustment'],2)
        ## 要加一個有沒有滿足的dictonary
        self.ratio_dic = {'ASSIGN_LIMIT_LOWER':{},'ASSIGN_LIMIT_UPPER':{}}
        name = range(len(list(self.df1[self.df1['Adjustment']<=0]['DISTR_ID'].values)))
        name_U = range(len(list(self.df1[self.df1['Adjustment_U']<=0]['DISTR_ID'].values)))
        name_list = list(self.df1[self.df1['Adjustment']<=0]['DISTR_ID'].values)
        conn = self.df1[self.df1['Adjustment']<=0].copy()
        name_list_U = list(self.df1[self.df1['Adjustment_U']<=0]['DISTR_ID'].values)
        conn_U = self.df1[self.df1['Adjustment_U']<=0].copy()
        for i in name_U:
            cond = abs(int(conn_U[conn_U['DISTR_ID']==name_list_U[i]]['Adjustment_U']))
            count = 0
            count_1 = 0
            ## 會有兩次更改第一次會先更改超過上限的訂單，第二次則是會更改超出下限的訂單
            for j in tqdm(range(len(self.df))):
                if self.df.loc[j,'REMARK'] == 'Can not find the corresponding shipper' or self.df.loc[j,'REMARK'] == 'There is no cuft volume for this order':
                    continue
                count_1+=1
                if count > cond:
                    self.logger.info('變更配送商：{}'.format(name_list_U[i]))
                    self.logger.info('變更訂單數量：{}'.format(count))
                    break
                elif count_1 == len(self.df):
                    self.logger.info('變更配送商：{}'.format(name_list_U[i]))
                    self.logger.info('變更訂單數量：{}'.format(count))
                elif self.df.loc[j,'EST_DISTR_TYPE_ID'] in self.DIS_TYPE[int(name_list_U[i])]:
                    chang_1 = 999
                    for k in range(len(self.DIS_TYPE[self.df1[self.df1['Adjustment_U']<=0]['DISTR_ID'].values[0]])):
                        list_des = self.df.loc[j]['符合條件配送商'].copy()
                        list_des = list(map(str,list_des))
                        list_chang = list(filter(lambda uu: str(self.DIS_TYPE[self.df1[self.df1['Adjustment_U']<=0]['DISTR_ID'].values[0]][k]) not in uu,str(list_des)))
                        list_chang = list(set(list_chang)&set(list(filter(lambda uu: str(k) not in uu,list_des))))
                    for p in list(self.df1[self.df1['Adjustment_U']<=0]['DISTR_ID'].values)[1:]:
                        # print(self.DIS_TYPE[int(p)])
                        for y in self.DIS_TYPE[int(p)]:
                            # print(y)
                            list_chang = list(set(list_chang)&set(list(filter(lambda uu: str(y) not in uu,list_des))))         
                    for q in list_chang:
                        volume = 0
                        chang = 0
                        for x in self.df.loc[j]['才績級距']: 
                            volume_compare = int(self.ori_money[self.ori_money['DISTR_TYPE_ID'] == self.df.loc[j]['EST_DISTR_TYPE_ID']][str(x)])
                            volume = int(self.ori_money[self.ori_money['DISTR_TYPE_ID'] == int(q)][str(x)]) + volume
                            chang = volume - volume_compare
                        if chang<chang_1:
                            self.df1.loc[(self.df1[self.df1['DISTR_ID'] == name_list_U[i]].index),'count'] = float(self.df1.loc[(self.df1[self.df1['DISTR_ID'] == list(conn_U['DISTR_ID'].values)[i]].index),'count'])-1
                            self.df1.loc[(self.df1[self.df1['DISTR_ID'] == name_list_U[i]].index),'Adjustment'] = float(self.df1.loc[(self.df1[self.df1['DISTR_ID'] == list(conn_U['DISTR_ID'].values)[i]].index),'Adjustment'])+1
                            self.df1.loc[(self.df1[self.df1['DISTR_ID'] == name_list_U[i]].index),'Adjustment_U'] = float(self.df1.loc[(self.df1[self.df1['DISTR_ID'] == list(conn_U['DISTR_ID'].values)[i]].index),'Adjustment_U'])+1
                            chang_1 = chang
                            self.df.loc[j,'EST_DISTR_TYPE_ID'] = int(q)
                            for s in list(group.size().keys()):
                                if s in [int(q)]:
                                    ccc = s
                            for jj in list(self.DIS_TYPE):
                                if ccc in self.DIS_TYPE[jj]:
                                    changgg = jj
                            self.df1.loc[(self.df1[self.df1['DISTR_ID'] == changgg].index),'count'] = float(self.df1.loc[(self.df1[self.df1['DISTR_ID'] == changgg].index),'count'])+1
                            self.df1.loc[(self.df1[self.df1['DISTR_ID'] == changgg].index),'Adjustment'] = float(self.df1.loc[(self.df1[self.df1['DISTR_ID'] == changgg].index),'Adjustment'])-1
                            self.df1.loc[(self.df1[self.df1['DISTR_ID'] == changgg].index),'Adjustment_U'] = float(self.df1.loc[(self.df1[self.df1['DISTR_ID'] == changgg].index),'Adjustment_U'])-1
                            count+=1
                            self.df1['Adjustment_U'] = round(self.df1['Adjustment_U'],2)
                            self.df1['Adjustment'] = round(self.df1['Adjustment'],2)
        for i in name:
            cond = abs(int(conn[conn['DISTR_ID']==name_list[i]]['Adjustment']))
            count = 0
            count_1 = 0
            for j in tqdm(range(len(self.df))):
                if self.df.loc[j,'REMARK'] == 'Can not find the corresponding shipper' or self.df.loc[j,'REMARK'] == 'There is no cuft volume for this order':
                    continue
                count_1+=1
                if count > cond:
                    self.logger.info('變更配送商：{}'.format(name_list[i]))
                    self.logger.info('變更訂單數量：{}'.format(count))
                    break
                elif count_1 == len(self.df):
                    self.logger.info('變更配送商：{}'.format(name_list[i]))
                    self.logger.info('變更訂單數量：{}'.format(count))
                elif self.df.loc[j,'EST_DISTR_TYPE_ID'] in self.DIS_TYPE[int(name_list[i])]:
                    chang_1 = 999
                    for k in range(len(self.DIS_TYPE[self.df1[self.df1['Adjustment']<=0]['DISTR_ID'].values[0]])):
                        list_des = self.df.loc[j]['符合條件配送商'].copy()
                        list_des = list(map(str,list_des))
                        list_chang = list(filter(lambda uu: str(self.DIS_TYPE[self.df1[self.df1['Adjustment']<=0]['DISTR_ID'].values[0]][k]) not in uu,list_des))
                        list_chang = list(set(list_chang)&set(list(filter(lambda uu: str(k) not in uu,list_des))))
                    for p in list(self.df1[self.df1['Adjustment']<=0]['DISTR_ID'].values)[1:]:
                        # print(self.DIS_TYPE[int(p)])
                        for y in self.DIS_TYPE[int(p)]:
                            # print(y)
                            list_chang = list(set(list_chang)&set(list(filter(lambda uu: str(y) not in uu,list_des))))         
                    for q in list_chang:
                        volume = 0
                        chang = 0
                        for x in self.df.loc[j]['才績級距']: 
                            volume_compare = int(self.ori_money[self.ori_money['DISTR_TYPE_ID'] == self.df.loc[j]['EST_DISTR_TYPE_ID']][str(x)])
                            volume = int(self.ori_money[self.ori_money['DISTR_TYPE_ID'] == int(q)][str(x)]) + volume
                            chang = volume - volume_compare
                        if chang<chang_1:
                            self.df1.loc[(self.df1[self.df1['DISTR_ID'] == name_list[i]].index),'count'] = float(self.df1.loc[(self.df1[self.df1['DISTR_ID'] == list(conn['DISTR_ID'].values)[i]].index),'count'])-1
                            self.df1.loc[(self.df1[self.df1['DISTR_ID'] == name_list[i]].index),'Adjustment'] = float(self.df1.loc[(self.df1[self.df1['DISTR_ID'] == list(conn['DISTR_ID'].values)[i]].index),'Adjustment'])+1
                            self.df1.loc[(self.df1[self.df1['DISTR_ID'] == name_list[i]].index),'Adjustment_U'] = float(self.df1.loc[(self.df1[self.df1['DISTR_ID'] == list(conn['DISTR_ID'].values)[i]].index),'Adjustment_U'])+1
                            chang_1 = chang
                            self.df.loc[j,'EST_DISTR_TYPE_ID'] = int(q)
                            for s in list(group.size().keys()):
                                if s in [int(q)]:
                                    ccc = s
                            for jj in list(self.DIS_TYPE):
                                if ccc in self.DIS_TYPE[jj]:
                                    changgg = jj
                            self.df1.loc[(self.df1[self.df1['DISTR_ID'] == changgg].index),'count'] = float(self.df1.loc[(self.df1[self.df1['DISTR_ID'] == changgg].index),'count'])+1
                            self.df1.loc[(self.df1[self.df1['DISTR_ID'] == changgg].index),'Adjustment'] = float(self.df1.loc[(self.df1[self.df1['DISTR_ID'] == changgg].index),'Adjustment'])-1
                            self.df1.loc[(self.df1[self.df1['DISTR_ID'] == changgg].index),'Adjustment_U'] = float(self.df1.loc[(self.df1[self.df1['DISTR_ID'] == changgg].index),'Adjustment_U'])-1
                            self.df1['Adjustment_U'] = round(self.df1['Adjustment_U'],2)
                            self.df1['Adjustment'] = round(self.df1['Adjustment'],2)
                            count+=1
        for i in list(group3.size().keys()):
            count_dis = 0
            for j in range(len(self.DIS_TYPE[i])):
                count_dis += self.ORDER_M_ratio[self.ORDER_M_ratio['EST_DISTR_TYPE_ID']==self.DIS_TYPE[i][j]]['EST_DISTR_TYPE_ID'].count()
            self.ratio_dic['ASSIGN_LIMIT_LOWER'][i] = int(group3.get_group(i)['ASSIGN_LIMIT_LOWER']) - count_dis
            self.ratio_dic['ASSIGN_LIMIT_UPPER'][i] = int(group3.get_group(i)['ASSIGN_LIMIT_UPPER'])
        self.logger.info('Order Distribution Status Statistics')
        for i in range(len(self.df1)):
            self.logger.info('{}:{}'.format(self.df1.loc[i]['DISTR_ID'],self.df1.loc[i]['count']))

    



        

    



    
    
