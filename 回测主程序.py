# -*- coding: utf-8 -*-
"""
Created on Tue Jul 03 15:15:13 2018

计算逻辑：
    1、根据调仓期，在每个调仓期初获取因子值，并根据因子值大小排序，并分为5组。
    2、取每个调仓期间的个股行情价格，并根据每日涨跌幅，计算每个股票的当日净值，按照分组计算每组净值。
    3、根据每组净值情况，计算每组的净值涨跌幅。
    4、将所有调仓期的净值数据合并，按照分组计算净值。

@author: yangchg
"""
import pandas as pd  
from sqlalchemy import create_engine 
import numpy as np 
import matplotlib.pyplot as plt 
import matplotlib.dates as mdate
import datetime,os

import sys
sys.path.append("D:\Program Files\Tinysoft\Analyse.NET") 
reload(sys)    
sys.setdefaultencoding('utf8')
import TSLPy2

def dealData(bk,begd,endd,adjustPeriods,factorsInfo,FactorName):
    #数据库连接引擎
    
    tableName = factorsInfo.get("tableName")
    direction = factorsInfo.get("direction")
    
    engine = create_engine('mysql://root:root@127.0.0.1/dwlh?charset=utf8') 
    
    #从天软中获取收盘价信息，采用前复权方式 
    closePrice = pd.DataFrame(TSLPy2.RemoteCallFunc('getClosePrice',[bk,begd,endd],{})[1]) 
    closePrice.date = pd.to_datetime(closePrice.date)
    
    adjustDatas = []
    netValues = [] 
    factorGroup = []
    #循环每次调仓日期
    for i in adjustPeriods.index[:-1]:
        
        adjustDay = adjustPeriods.ix[i,"date"]
        nextAdjustDay = adjustPeriods.ix[i,"nextAdjustDay"] 
        print u"处理调仓日 "+adjustDay
        #取得因子值 
        sql = "select con_date,stock_code,{FactorName} from {tableName} where con_date = date('{con_date}');".format(FactorName=FactorName,tableName=tableName,con_date=adjustDay)
        # read_sql_query的两个参数: sql语句， 数据库连接 
        factor = pd.read_sql_query(sql, engine) 
        
        #按照调仓日期取板块信息
        BKStocks = TSLPy2.RemoteCallFunc('getbkByName',[bk,TSLPy2.EncodeDate(int(adjustDay[:4]),int(adjustDay[5:7]),int(adjustDay[8:10]))],{}) 
        BKStocks = pd.DataFrame(BKStocks[1],columns=["TSLCode"])
        BKStocks["stock_code"] = BKStocks["TSLCode"].apply(lambda x :x[2:]) 
        
        #对因子值按照从小打到排序分组，分为5组
        factor = factor.merge(BKStocks,on="stock_code")
        factor["group"] = pd.qcut(factor[FactorName].rank(method='first',ascending= (not direction)),5,labels=np.arange(1,6))
        factor.con_date = pd.to_datetime(factor.con_date) 
        
        #取得期间股票行情
        adjustClosePrice = closePrice[(closePrice["date"]>adjustDay) & (closePrice["date"]<=nextAdjustDay)]
        factorMergeClose = factor.merge(adjustClosePrice,how='right',left_on=["stock_code"],right_on=["stock_code"])
        factorMergeClose = factorMergeClose.groupby(["stock_code"],group_keys=False).apply(lambda x : x.sort_values(by=["date"]).fillna(method="pad").dropna())
        factorMergeClose["stockNetValues"] = factorMergeClose.groupby(["stock_code"])["stockzf"].apply(lambda x :(x/100+1).cumprod())
        
        groupNetval = factorMergeClose.groupby(["date","group"])["stockNetValues"].mean().unstack(1) 
        groupNetval1 = groupNetval.ix[:1,]-1
        groupNetval = groupNetval.pct_change().fillna(value=groupNetval1) 
        
        factorGroup.append(factor)
        netValues.append(groupNetval)
        adjustDatas.append(factorMergeClose)
        
    netValues = pd.concat(netValues)
    netValues = (netValues+1).cumprod()
    netValues.plot(figsize=(18,10),title=FactorName)    
    Path = datetime.datetime.now().strftime("%Y-%m-%d")
    
    if os.path.exists(Path)  and os.access(Path, os.R_OK):
        print Path , ' is exist!' 
    else:
        os.makedirs(Path) 
    
    pd.concat(adjustDatas).to_excel(Path+"\\"+FactorName+"_adjustDatas.xlsx")
    pd.concat(factorGroup).to_excel(Path+"\\"+FactorName+"_factorGroup.xlsx")
    plt.savefig(Path+"\\"+FactorName+".png")

    
if __name__ == '__main__':
    
    #获取调仓周期，周期分为月度和周度可以选择
    begd=TSLPy2.EncodeDate(2017,12,1)
    endd=TSLPy2.EncodeDate(2018,07,05)
    adjustPeriods = TSLPy2.RemoteCallFunc('getAdjustPeriod',[begd,endd,u"月线"],{})
    adjustPeriods = pd.DataFrame(adjustPeriods[1])
    adjustPeriods["nextAdjustDay"] = adjustPeriods["date"].shift(-1)
    
    #板块名称
    bk = u"申万医药生物" 
    factors = {"score":{"tableName":"stock_score_all","direction":1},
               "con_peg":{"tableName":"t_factor_value_all","direction":1},
               "con_pe":{"tableName":"t_factor_value_all", "direction":1},
               "con_eps":{"tableName":"t_factor_profit_all", "direction":1},
               #"con_roe":{"tableName":"t_factor_profit_all", "direction":1},
               #"con_np_yoy":{"tableName":"t_factor_growth_all", "direction":1},
               #"con_or_yoy":{"tableName":"t_factor_growth_all", "direction":1},
               #"con_npcgrate_4w":{"tableName":"t_factor_growth_all", "direction":1},
               }
    
    for FactorName,factorsInfo in factors.iteritems():
        dealData(bk,begd,endd,adjustPeriods,factorsInfo,FactorName) 
    

    
    #dajustDataFrame是分组数据    
    #dajustDataFrame = pd.concat(netValues)
    #dajustDataFrame = dajustDataFrame.reset_index(drop=True)
    #dajustDataFrame.con_date = pd.to_datetime(dajustDataFrame.con_date)

    #adjustDataClosePrice = dajustDataFrame.merge(closePrice,how='right',left_on=["con_date","stock_code"],right_on=["date","stock_code"])
    
    #先按照股票进行分组，在分组中按照时间排序后，向下填充空值，并删除最上面为空值的数据
    #adjustDataClosePrice = adjustDataClosePrice.groupby(["stock_code"],group_keys=False).apply(lambda x : x.sort_values(by=["date"]).fillna(method="pad").dropna())
    
    #按照股票代码和组别分组，分别计算在各组中的股票净值
    #dajustDataFrame["NatValue"] = dajustDataFrame.groupby(["stock_code","con_date"])["stockzf"].apply(lambda x :(x/100+1).cumprod())
    #按照调仓期分为不同的分组
    #groupkey = dajustDataFrame[["date","con_date",]].drop_duplicates().set_index("date")["con_date"]
    
    #将数据按照日期和组别分组，计算每日组内的净值情况
    #groupNatValue = dajustDataFrame.groupby(["date","group"])["NatValue"].mean().unstack(1)
    
    """
    fig = plt.figure(figsize=(18,10))
    ax = fig.add_subplot(1,1,1)
    
    ax.set_title(factors.get(u"确定性因子"))
    ax.xaxis.set_major_formatter(mdate.DateFormatter('%Y-%m-%d'))
    
    #plt.gca().xaxis.set_major_formatter(mdate.DateFormatter('%Y-%m-%d'))
    #plt.gcf().autofmt_xdate()
    #ax.set_xticks(groupNatValue.index)
    ax.set_xticklabels(groupNatValue.index,rotation=30,fontsize='small')
    
    ax.plot(groupNatValue,label=groupNatValue.columns)
    """
        
        
        
    
    
    
 