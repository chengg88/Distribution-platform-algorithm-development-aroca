import json, time
from typing import List, Dict, Union
from fastapi import Cookie, Body, FastAPI, File, UploadFile, Request, HTTPException, Response, Depends, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.logger import logger as fastapi_logger
from fastapi.responses import RedirectResponse, PlainTextResponse
from fastapi.responses import FileResponse, StreamingResponse
from fastapi import Security
from fastapi.security.api_key import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.exceptions import RequestValidationError
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
import logging
from pydantic import BaseModel, Field
import os
import sys
import requests

from starlette.routing import request_response
import base64
from datetime import datetime
from io import StringIO, BytesIO
from fastapi_login import LoginManager
from fastapi.security import OAuth2PasswordRequestForm
from fastapi_login.exceptions import InvalidCredentialsException
from collections import OrderedDict
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from sklearn.preprocessing import normalize

from manager import dms_manager, dms_data_manager
from common import rule_based, metaheuristic
from config import global_var

import pymssql
import numpy as np
import pandas as pd
import itertools
import func_timeout

'''
新增HTTP Header相關安全性設定
'''
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add security headers to all responses."""

    def __init__(self, app: FastAPI, csp: bool = True) -> None:
        """Init SecurityHeadersMiddleware.
        :param app: FastAPI instance
        :param no_csp: If no CSP(Content Security Policy) should be used;
            defaults to :py:obj:`False`
        """
        super().__init__(app)
        self.csp = csp

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        """Dispatch of the middleware.
        :param request: Incoming request
        :param call_next: Function to process the request
        :return: Return response coming from from processed request
        """
        headers = {
            "Content-Security-Policy": "" if not self.csp else parse_policy(CSP),
            "Cross-Origin-Opener-Policy": "same-origin",
            "Referrer-Policy": "strict-origin-when-cross-origin",
            "Strict-Transport-Security": "max-age=31556926; includeSubDomains",
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "X-XSS-Protection": "1; mode=block",
            "Cache-Control": "no-store",
            "Pragma": "no-cache",
        }
        response = await call_next(request)
        response.headers.update(headers)

        return response

if os.path.isdir('c:\\Logfiles'):
    handler = TimedRotatingFileHandler('.\\logs\\dms.log', when="midnight", interval=1, encoding="utf-8", backupCount=9)
else:    
    handler = TimedRotatingFileHandler('.\\logs\\dms.log', when="midnight", interval=1, encoding="utf-8", backupCount=9)

logging.getLogger().setLevel(logging.NOTSET)
fastapi_logger.addHandler(handler)
formatter = logging.Formatter("[%(asctime)s.%(msecs)03d] %(levelname)s [%(thread)d] - %(message)s", "%Y-%m-%d %H:%M:%S")
handler.setFormatter(formatter)

fastapi_logger.addHandler(logging.StreamHandler(sys.stdout))
gunicorn_logger = logging.getLogger('gunicorn.error')
for h in gunicorn_logger.handlers:
    fastapi_logger.addHandler(h)

app = FastAPI()
app.add_middleware(SecurityHeadersMiddleware, csp=True)

origins = [
    "http://localhost:8080"
]

### Database connection and outlying island config
cfg_path = './config/config.json'
with open(cfg_path, 'r', encoding='utf-8-sig') as f:
    cfg = json.load(f)
# databse connection information
db_cfg = cfg['db_connect']
db_cfg['creator'] = __import__(db_cfg['creator'])
# outlying island information
outly_island_cfg = cfg['islands']

class Item(BaseModel):
    batchNo: str

def check_batch_no(batchNo, logger=fastapi_logger, config=db_cfg):
    manager = dms_manager.DMSManager(logger, **config)
    result = manager.check_db_data('ORDER_M', batchNo)[0][0]

    if result == 0:
        logger.info('There\'s not orders with this batch number')
        response = {"returnCode": "E101", "returnMsg": "資料庫查無該批號"}
    else:
        response = {"returnCode": "S200", "returnMsg": ""}
    return response

def rule_based_algo(batchNo, logger=fastapi_logger, config=db_cfg):
    ### 取得資料庫資料
    logger.info('DMS呼叫演算法計算對應訂單批號配送商')
    use_cols = ['ID','BATCH_NO','ORDER_NO','ORDER_DATE','SHIPPER_CODE','SPECIFY_ARRIVAL_DATE','RECEIVE_ZIP_CODE','CREATE_DT','IS_SA','IS_FREEZE','COD','EST_DISTR_TYPE_ID','REMARK']
    DM = dms_data_manager.DMSDataManager(logger, config, use_cols)
    df, DISTR_INFO, ORDER_M_ratio, SHIPPER_INFO, ori_money, holiday, ratio = DM.read_data_from_db(batchNo)

    ### 建立演算法
    com_rule = rule_based.rule(df, DISTR_INFO, ORDER_M_ratio, SHIPPER_INFO, ori_money, holiday, ratio, outly_island_cfg, logger)
    com_rule.df['REMARK'] = ""
    com_rule.df['EST_DISTR_TYPE_ID'] = "NULL"
    com_rule.build_limitation()
    com_rule.find_cheapest_carrier()
    com_rule.adjustment_ratio()
    logger.info('rule-based最適配送商演算法計算完成')

    ### 將訂單資料進行分類(有無符合條件配送商)
    if len(com_rule.df) != 0: # 先確認是否有訂單
        del_ind = [val == [] for val in com_rule.df['符合條件配送商']] # 判斷符合條件配送商為空值的結果
        no_est_df = com_rule.df[del_ind]
        com_rule.df = com_rule.df.drop(np.where(del_ind)[0])
        com_rule.df = com_rule.df.reset_index(drop=True)
        no_est_df = no_est_df.reset_index(drop=True)
    else:
        no_est_df = pd.DataFrame()

    return com_rule, DISTR_INFO, SHIPPER_INFO, no_est_df

def GA_algo(com_rule, DISTR_INFO, logger=fastapi_logger, config=db_cfg):
    global suitable_deli
    # 找不到符合條件配送商，便不需做GA
    if len(com_rule.df) != 0:
        possible_ans = com_rule.df['符合條件配送商'].tolist() #可行配送商
        best_deli = com_rule.df['EST_DISTR_TYPE_ID'].astype('int').tolist() #最適配送商
        suitable_deli = best_deli
        port_f = 0.7 #運費比例
        port_t = 0.15 #速度比例
        port_q = 0.15 #品質比例
        box_num = com_rule.df['箱數'].tolist() #箱數
        ori_vol = com_rule.df['才績級距'].tolist() #訂單材積
        #調整訂單材積(離島)
        outly_ind = com_rule.df.index[com_rule.df['RECEIVE_ZIP_CODE'].astype('int').isin(outly_island_cfg)].tolist()
        if len(outly_ind) > 0:
            for ind in outly_ind:
                ori_vol[ind] = 'OUTLYINGIS'
        # 抓取材積對應費用資料表
        manager = dms_manager.DMSManager(fastapi_logger, **config)
        fee_info = manager.get_db_prc('DISTR_FEE_INFO').fillna(value=np.nan)
        # 將不同材積的運費做成各別的dict
        fee_ID = fee_info['DISTR_TYPE_ID'].tolist()
        numtype_fee = [] #用欄位名稱的資料型態去抓運費的欄位
        for find_col in range(len(fee_info.columns)):
            type_col = fee_info.columns[find_col].isdigit()
            if type_col == True:
                numtype_fee.append(fee_info.columns[find_col])
        fee_list = {} #建立各才績對應的運費
        for each_feecol in numtype_fee:
            nor_fee = 1-(normalize(fee_info[each_feecol][~pd.isnull(fee_info[each_feecol])].astype('Int32').values.reshape(1,-1)).flatten()+0.01) #正規化後用1減，讓運費最小的值變成最大的
            volfee_ID = np.array(fee_ID)[fee_info[each_feecol][~pd.isnull(fee_info[each_feecol])].index.tolist()].tolist()
            fee_list[each_feecol] = dict(zip(volfee_ID, nor_fee))
        fee_list['OTHER'] = dict(zip(np.array(fee_ID)[fee_info['OTHER'][~pd.isnull(fee_info['OTHER'])].index.tolist()].tolist(), (1-(normalize(fee_info['OTHER'][~pd.isnull(fee_info['OTHER'])].astype('Int32').values.reshape(1, -1)).flatten()+0.01)))) # 超過150運費
        fee_list['OUTLYINGIS'] = dict(zip(np.array(fee_ID)[fee_info['OUTLYINGIS'][~pd.isnull(fee_info['OUTLYINGIS'])].index.tolist()].tolist(), (1-(normalize(fee_info['OUTLYINGIS'][~pd.isnull(fee_info['OUTLYINGIS'])].astype('Int32').values.reshape(1, -1)).flatten()+0.01)))) # 離島運費
        
        # 取得品質、時效評分
        distr_m = manager.get_db_table('DISTR_M')
        scores = pd.DataFrame(distr_m,columns=['ID','QUALITY_SCORE', 'TIMES_SCORE'])
        ID = scores['ID'].tolist()
        ori_quality = scores['QUALITY_SCORE'].astype('int').values.reshape(1,-1)
        ori_times = scores['TIMES_SCORE'].astype('int').values.reshape(1,-1)
        # 取得上下限
        upper_bound = com_rule.ratio_dic['ASSIGN_LIMIT_UPPER'].copy() #上限(訂單數)
        lower_bound = com_rule.ratio_dic['ASSIGN_LIMIT_LOWER'].copy() #下限(訂單數)
        # 調整下限(下限比例與符合條件配送商中數量取小)
        possible_list = sum(possible_ans,[]) #possible ans 2d to 1d
        possible_num = {}
        deli_type = []
        for element in possible_list:
            each_deli_type = DISTR_INFO.loc[DISTR_INFO['DISTR_TYPE_ID'] == element , ['DISTR_ID']].values[0].tolist()
            deli_type.append(each_deli_type)
        deli_type = sum(deli_type, []) #2d list to 1d
        for each_eles in deli_type: #計算各元素於符合條件配送商中的數量
            possible_num[each_eles] = deli_type.count(each_eles)
        for eles in lower_bound.keys():
            if eles not in possible_num.keys():
                if lower_bound[eles] != 0: #小於0(負數)須調整為0；大於0但可行解中無該元素則亦調整為0
                    lower_bound[eles] = 0
            else:
                if lower_bound[eles] < 0:
                    lower_bound[eles] = 0
                else:
                    lower_bound[eles] = min(lower_bound[eles],possible_num[eles])
        ## 執行演算法
        com_GA = metaheuristic.GA(suitable_deli)
        com_GA.execute(best_deli, possible_ans, DISTR_INFO, box_num, upper_bound, lower_bound, port_f, port_t, port_q, fee_list, ori_vol, ori_times, ori_quality, ID, cfg['GA_hyperparameter'])
        return global_var.get_value('suitable_deli')

def limited_running(f, max_wait, args):
    try:
        return func_timeout.func_timeout(max_wait, f, args)
    except func_timeout.FunctionTimedOut:
        pass

def save_in_DB(com_rule, no_est_df, SHIPPER_INFO, logger=fastapi_logger, config=db_cfg):
    ### 將配送資料寫回資料庫
    manager = dms_manager.DMSManager(logger, **config)
    logger.info('Start to write the result to database')
    suitable_deli = global_var.get_value('suitable_deli')
    # 將 GA 計算結果寫回資料庫
    for ind in range(len(com_rule.df)):
        temp_val = com_rule.df.loc[ind, ['BATCH_NO', 'ORDER_NO', 'EST_DISTR_TYPE_ID', 'REMARK']].copy()
        temp_val['EST_DISTR_TYPE_ID'] = suitable_deli[ind]
        manager.fill_distr(*temp_val)
    # 將無法派送結果寫回資料庫
    shipper_default_res =  SHIPPER_INFO.loc[:, ['SHIPPER_CODE', 'DEFAULT_DISTR_TYPE_ID']].drop_duplicates(subset=['SHIPPER_CODE'])
    # no_distr_order = dict() # 紀錄無法派送訂單結果傳給DMS
    for ind in range(len(no_est_df)):
        temp_val = no_est_df.loc[ind, ['BATCH_NO', 'ORDER_NO', 'EST_DISTR_TYPE_ID', 'REMARK']].copy()
        try:
            temp_val['EST_DISTR_TYPE_ID'] = shipper_default_res.loc[shipper_default_res['SHIPPER_CODE'] == no_est_df.loc[ind, 'SHIPPER_CODE'], 'DEFAULT_DISTR_TYPE_ID'].values[0]
        except:
            temp_val['EST_DISTR_TYPE_ID'] = "NULL"
        manager.fill_distr(*temp_val)
    logger.info('Finish writing the result to database')

    ### 將配送結果寫入log(rule-based、GA結果)
    pd.set_option('display.width', 500)
    pd.set_option('max_colwidth', 200)
    logger.info('rule-based and GA result')
    if len(com_rule.df):
        com_rule.df['EST_DISTR_TYPE_ID(GA_result)'] = suitable_deli[0]
        logger.info(com_rule.df.loc[:, ['BATCH_NO', 'ORDER_NO', 'EST_DISTR_TYPE_ID', 'EST_DISTR_TYPE_ID(GA_result)', 'REMARK']])
    if len(no_est_df):
        logger.info(no_est_df.loc[:, ['BATCH_NO', 'ORDER_NO', 'EST_DISTR_TYPE_ID', 'REMARK']])

    ### 呼叫 DMS API 告知寫回資料庫訊息
    hdr = {"Content-Type": "application/json"}
    # response = requests.post('http://10.71.253.133:8080/api/calcBatchNoDone', data=json.dumps({"sysId": "ALGORITHM", "batchNo": batchNo, "no_distr_order": no_distr_order}), headers=hdr)
    response = requests.post('http://10.71.253.133:8080/api/calcBatchNoDone', data=json.dumps({"sysId": "ALGORITHM", "batchNo": batchNo}), headers=hdr)
    if response.json()['code'] == 'S000':
        logger.info('Finish call DMS API, the message is shown below:')
        logger.info(response.text)
    else:
        logger.info('Try to call DMS API, the message is shown below:')
        logger.warn(response.text)

def main(batchNo):
    star_time = time.time()
    global_var._init()
    com_rule, DISTR_INFO, SHIPPER_INFO, no_est_df = rule_based_algo(batchNo)
    best_deli = com_rule.df['EST_DISTR_TYPE_ID'].astype('int').tolist()
    rule_time = time.time()
    global_var.set_value('suitable_deli', best_deli)
    rest_of_time = cfg['running_time'] - (rule_time - star_time)
    print("time:", rest_of_time)
    if rest_of_time > 0:
        limited_running(GA_algo, rest_of_time, [com_rule, DISTR_INFO])
    save_in_DB(com_rule, no_est_df, SHIPPER_INFO)

@app.get("/")
async def root():
    return {"message": "Hello world"}

@app.post("/api/addBatchNo")
async def comp_distributor(item: Item, backgroundTasks: BackgroundTasks):
    response = check_batch_no(item.batchNo)
    if response['returnCode'] == 'S200':
        backgroundTasks.add_task(main, item.batchNo)
    return response

CSP: Dict[str, Union[str, List[str]]] = {
    "default-src": ["'self'", "cdn.jsdelivr.net"],
    "img-src": [
        "*",
        # For SWAGGER UI
        "data:",
    ],
    # "connect-src": "'self'",
    # "script-src": "'self'",
    # "style-src": ["'self'", "'unsafe-inline'"],
    "script-src-elem": [
        # For SWAGGER UI
        "https://cdn.jsdelivr.net/npm/swagger-ui-dist@4/swagger-ui-bundle.js",
        "'sha256-1I8qOd6RIfaPInCv8Ivv4j+J0C6d7I8+th40S5U/TVc='",
    ],
    "style-src-elem": [
        # For SWAGGER UI
        "https://cdn.jsdelivr.net/npm/swagger-ui-dist@4/swagger-ui.css",
    ],
}

def parse_policy(policy: Dict[str, Union[str, List[str]]]) -> str:
    """Parse a given policy dict to string."""
    if isinstance(policy, str):
        # parse the string into a policy dict
        policy_string = policy
        policy = OrderedDict()

        for policy_part in policy_string.split(";"):
            policy_parts = policy_part.strip().split(" ")
            policy[policy_parts[0]] = " ".join(policy_parts[1:])

    policies = []
    for section, content in policy.items():
        if not isinstance(content, str):
            content = " ".join(content)
        policy_part = f"{section} {content}"

        policies.append(policy_part)

    parsed_policy = "; ".join(policies)

    return parsed_policy