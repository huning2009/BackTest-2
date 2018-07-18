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
import  seaborn as sns 
import statsmodels.api as sm
from patsy import dmatrices

import sys
sys.path.append("D:\Program Files\Tinysoft\Analyse.NET") 
reload(sys)    
sys.setdefaultencoding('utf8')
import TSLPy2

def dealData(bk,begd,endd,adjustPeriods,factorsInfo,FactorName,Path):
    #数据库连接引擎
    
    tableName = factorsInfo.get("tableName")
    direction = factorsInfo.get("direction") #因子方向1为正序，0位逆序
    reciprocal = factorsInfo.get("reciprocal") #因子值是否取倒数
    isLogDeal = factorsInfo.get("isLogDeal") #因子是否进行ln处理
    
    engine = create_engine('mysql://root:root@172.16.158.142/dwlh?charset=utf8') 
    
    periedValues = []
    #循环每次调仓日期
    for i in adjustPeriods.index[:-1]:
        
        adjustDay = adjustPeriods.ix[i,"date"]
        nextAdjustDay = adjustPeriods.ix[i,"nextAdjustDay"] 

        #取得所有股票在调仓日因子值 
        sql = "select con_date,stock_code,{FactorName} from {tableName} where con_date = date('{con_date}');".format(FactorName=FactorName,tableName=tableName,con_date=adjustDay)
        # read_sql_query的两个参数: sql语句， 数据库连接 
        factor = pd.read_sql_query(sql, engine)
        
        #将日期字段设置为日期类型
        factor.con_date = pd.to_datetime(factor.con_date) 
        
        #按照调仓日期取板块信息，天软函数getbkByName，会剔除调仓日一字涨跌停、停牌以及上市时间小于120日的股票
        BKStocks = TSLPy2.RemoteCallFunc('getbkByName2',[bk,TSLPy2.EncodeDate(int(adjustDay[:4]),int(adjustDay[5:7]),int(adjustDay[8:10]))],{}) 
        BKStocks = pd.DataFrame(BKStocks[1])
        BKStocks["SWNAME"] = BKStocks["SWNAME"].apply(lambda x : x.decode('gbk'))
        BKStocks["stock_code"] = BKStocks["id"].apply(lambda x :x[2:]) 
        
        #对因子值和板块合并
        factor = factor.merge(BKStocks,on="stock_code")
        #判断是否对因子值进行倒序处理
        if reciprocal ==1 : 
            factor[FactorName]=factor[FactorName].apply(lambda x : 1/x  if x<>0 else x ) 
            
        if isLogDeal ==1 :
            factor[FactorName]=factor[FactorName].apply(np.log) 
        
        #替换异常值
        factorMedia = factor[FactorName].median() 
        MAD =  (factor[FactorName]-factorMedia).apply(abs).median()   
        factor.loc[factor[FactorName]>(factorMedia+3*1.4826*MAD),FactorName]= factorMedia+3*1.4826*MAD
        factor.loc[factor[FactorName]<(factorMedia-3*1.4826*MAD),FactorName]= factorMedia-3*1.4826*MAD
        
        #zscore标准化
        factorMean = factor[FactorName].mean()
        factorStd = factor[FactorName].std()
        factor[FactorName] = factor[FactorName].apply(lambda x : (x-factorMean)/factorStd)
        
        #下期收益序列：
        stokzf = pd.DataFrame(TSLPy2.RemoteCallFunc('getStockZF',[bk,TSLPy2.EncodeDate(int(adjustDay[:4]),int(adjustDay[5:7]),int(adjustDay[8:10])),
                                                                  TSLPy2.EncodeDate(int(nextAdjustDay[:4]),int(nextAdjustDay[5:7]),int(nextAdjustDay[8:10]))],{})[1])
        factor= factor.merge(stokzf,on="stock_code")
        factor.set_index("stock_code",inplace=True)
        
        #计算IC值和RANKIC值
        IC = factor["zf"].corr(factor[FactorName])
        rankIC = factor["zf"].corr(factor[FactorName],method="spearman")
        
        #回归进行行业中性处理
        factor = factor.dropna()
        y, X = dmatrices('zf ~ {factorName} + SWNAME'.format(factorName=FactorName), data=factor, return_type='dataframe')
        
        """
        y= factor["zf"]
        factor["cons"]=1.0
        colums = ["cons",FactorName] 
        X= factor[colums].join( pd.get_dummies(factor["SWNAME"])) #将行业设置为哑变量
        """
        #res = sm.OLS(y, X).fit() #通过OLS进行回归
        res2= sm.RLM(y, X).fit()  #通过RLM进行回归
        #res3= sm.WLS(y, X).fit() #通过WLS进行回归
        factorParam = res2.params[FactorName]
        factorT = res2.tvalues[FactorName]
        
        periedValues.append(pd.DataFrame({"FactorName":FactorName,
                      "adjustDay":adjustDay,
                      "IC":IC,
                      "rankIC":rankIC,
                      "factorbeta":factorParam,
                      "factorT":factorT},index=0))
        
        """
        fig, ax = plt.subplots(figsize=(8,6))

        ax.plot(factor["con_roe"], y, 'o', label="Data")
        #ax.plot(x["con_roe"], y_true, 'b-', label="True")
        ax.plot(factor["con_roe"], res2.fittedvalues, 'r--.', label="RLMPredicted")
        ax.plot(factor["con_roe"], res.fittedvalues, 'b--.', label="OLSPredicted")
        legend = ax.legend(loc="best")
        """
        
    return periedValues
        
    
if __name__ == '__main__':
    
    #获取调仓周期，周期分为月度和周度可以选择
    begd=TSLPy2.EncodeDate(2018,1,3)
    endd=TSLPy2.EncodeDate(2018,07,12)
    #设置调仓周期为月度调仓
    adjustPeriods = TSLPy2.RemoteCallFunc('getAdjustPeriod',[begd,endd,u"月线"],{})
    adjustPeriods = pd.DataFrame(adjustPeriods[1])
    adjustPeriods["nextAdjustDay"] = adjustPeriods["date"].shift(-1)
    #在当前目录下新增路径，如没有文件夹则新增文件夹
    Path = u"单因子检验\\"+datetime.datetime.now().strftime("%Y-%m-%d")
    if os.path.exists(Path)  and os.access(Path, os.R_OK):
        print Path , ' is exist!' 
    else:
        os.makedirs(Path) 
    #板块名称
    bk = u"A股" 
    factors = {#"tcap":{"tableName":"t_factor_scale_all","direction":1,"reciprocal":0,"isLogDeal":1},
               # "score":{"tableName":"stock_score_all","direction":1,"reciprocal":0},
               #"con_peg":{"tableName":"t_factor_value_all","direction":0,"reciprocal":0},
               #"con_pe":{"tableName":"t_factor_value_all", "direction":1,"reciprocal":1},
               #"con_eps":{"tableName":"t_factor_profit_all", "direction":1,"reciprocal":0},
               "con_roe":{"tableName":"t_factor_profit_all", "direction":1,"reciprocal":0},
              #"con_np_yoy":{"tableName":"t_factor_growth_all", "direction":1,"reciprocal":0},
              # "con_or_yoy":{"tableName":"t_factor_growth_all", "direction":1,"reciprocal":0},
               #"con_npcgrate_4w":{"tableName":"t_factor_growth_all", "direction":1,"reciprocal":0},
               }

    
    finalbx = []
    for FactorName,factorsInfo in factors.iteritems():
        finalbx.append(dealData(bk,begd,endd,adjustPeriods,factorsInfo,FactorName,Path) )
    result = pd.concat(finalbx)
    #pd.concat(finalbx).to_excel(Path+"\\"+bk+u"板块所有因子表现.xlsx")
    
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
        
        
        
    
    
    
 