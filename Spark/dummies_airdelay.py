#! /usr/bin/env python3
import findspark
findspark.init("/usr/lib/spark-current")
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os 
import pickle
spark = SparkSession.builder.appName("Python Spark with DataFrame").getOrCreate()

#schema
from pyspark.sql.types import *
schema_sdf = StructType([
        StructField('Year', IntegerType(), True),
        StructField('Month', IntegerType(), True),
        StructField('DayofMonth', IntegerType(), True),
        StructField('DayOfWeek', IntegerType(), True),
        StructField('DepTime', DoubleType(), True),
        StructField('CRSDepTime', DoubleType(), True),
        StructField('ArrTime', DoubleType(), True),
        StructField('CRSArrTime', DoubleType(), True),
        StructField('UniqueCarrier', StringType(), True),
        StructField('FlightNum', StringType(), True),
        StructField('TailNum', StringType(), True),
        StructField('ActualElapsedTime', DoubleType(), True),
        StructField('CRSElapsedTime',  DoubleType(), True),
        StructField('AirTime',  DoubleType(), True),
        StructField('ArrDelay',  DoubleType(), True),
        StructField('DepDelay',  DoubleType(), True),
        StructField('Origin', StringType(), True),
        StructField('Dest',  StringType(), True),
        StructField('Distance',  DoubleType(), True),
        StructField('TaxiIn',  DoubleType(), True),
        StructField('TaxiOut',  DoubleType(), True),
        StructField('Cancelled',  IntegerType(), True),
        StructField('CancellationCode',  StringType(), True),
        StructField('Diverted',  IntegerType(), True),
        StructField('CarrierDelay', DoubleType(), True),
        StructField('WeatherDelay',  DoubleType(), True),
        StructField('NASDelay',  DoubleType(), True),
        StructField('SecurityDelay',  DoubleType(), True),
        StructField('LateAircraftDelay',  DoubleType(), True)
    ])

air = spark.read.options(header='true').schema(schema_sdf).csv("/data/airdelay_small.csv")
air = air.select(['Arrdelay','Year','Month','DayofMonth','DayOfWeek','DepTime','CRSDepTime','CRSArrTime','UniqueCarrier','ActualElapsedTime','Origin','Dest','Distance'])
air = air.na.drop()
#air_1.coalesce(1).write.option('header','true').csv('data_bicheng')
#air = spark.read.options(header='true', inferSchema='true').csv("")


data = air.withColumn('Arrdelay',F.when(air['Arrdelay'] > 0, 1).otherwise(0))
'''大于0的为延误记作1，小于0的为未延误记作0
    PySpark数据框中添加新列的方法：https://blog.csdn.net/wulishinian/article/details/105817409
'''


#哑变量处理过程
import pandas as pd
import numpy as np
import sys
import re
from collections import Counter


def dummy_factors_counts(pdf, dummy_columns):
    '''Function to count unique dummy factors for given dummy columns
    pdf: pandas data frame
    dummy_columns: list. Numeric or strings are both accepted.
    return: dict same as dummy columns
    '''
    # Check if current argument is numeric or string
    pdf_columns = pdf.columns.tolist()  # Fetch data frame header

    dummy_columns_isint = all(isinstance(item, int) for item in dummy_columns)
    '''isinstance() 判断item是否是int
       all()用于判断给定的可迭代参数 iterable 中的所有元素是否都为 TRUE，
    如果是返回 True，否则返回 False
    '''
    if dummy_columns_isint:
        dummy_columns_names = [pdf_columns[i] for i in dummy_columns]
    else:
        dummy_columns_names = dummy_columns

    factor_counts = {}
    for i in dummy_columns_names:
        factor_counts[i] = (pdf[i]).value_counts().to_dict()
    #统计每一列里的不同值的个数
    return factor_counts


###合并两个字典，并计算同一key的和（两个字典都有子字典）
def cumsum_dicts(dict1, dict2):
    '''Merge two dictionaries and accumulate the sum for the same key where each dictionary
    containing sub-dictionaries with elements and counts.
    '''
    # If only one dict is supplied, do nothing.
    if len(dict1) == 0:
        dict_new = dict2
    elif len(dict2) == 0:
        dict_new = dict1
    else:
        dict_new = {}
        for i in dict1.keys():
            dict_new[i] = dict(Counter(dict1[i]) + Counter(dict2[i]))
            #counter是python计数器类，返回元素取值的字典,且按频数降序
    return dict_new


def select_dummy_factors(dummy_dict, keep_top, replace_with, pickle_file):
    '''Merge dummy key with frequency in the given file
    dummy_dict: dummy information in a dictionary format
    keep_top: list
    '''
    dummy_columns_name = list(dummy_dict)  #本身词典里就是取值
    # nobs = sum(dummy_dict[dummy_columns_name[1]].values())

    factor_set = {}  # The full dummy sets
    factor_selected = {}  # Used dummy sets
    factor_dropped = {}  # Dropped dummy sets
    factor_selected_names = {}  # Final revised factors

    for i in range(len(dummy_columns_name)):

        column_i = dummy_columns_name[i]    #给出列来

        factor_set[column_i] = list((dummy_dict[column_i]).keys())#第i列的可能取值表

        factor_counts = list((dummy_dict[column_i]).values())  #第i列的值的个数
        factor_cumsum = np.cumsum(factor_counts)           #累加
        factor_cumpercent = factor_cumsum / factor_cumsum[-1] #累积比率

        factor_selected_index = np.where(factor_cumpercent <= keep_top[i]) #top这个是给定的
        factor_dropped_index = np.where(factor_cumpercent > keep_top[i])

        factor_selected[column_i] = list(
            np.array(factor_set[column_i])[factor_selected_index]) #一列有一堆可用取值

        factor_dropped[column_i] = list(
            np.array(factor_set[column_i])[factor_dropped_index])

        # Replace dropped dummies with indicators like `others`
        if len(factor_dropped_index[0]) == 0:
            factor_new = []
        else:
            factor_new = [replace_with]

        factor_new.extend(factor_selected[column_i]) 
        #extend列表末尾一次性追加另一个序列中的多个值
        factor_selected_names[column_i] = [
            column_i + '_' + str(x) for x in factor_new
        ]

    dummy_info = {
        'factor_set': factor_set,
        'factor_selected': factor_selected,
        'factor_dropped': factor_dropped,
        'factor_selected_names': factor_selected_names
    }

    pickle.dump(dummy_info, open(pickle_file, 'wb'))
    print("dummy_info saved in:\t" + pickle_file)

    return dummy_info
    #返回了一个包含处理信息的字典


'''
pickle提供了一个简单的持久化功能。可以将对象以文件的形式存放在磁盘上
pickle.dump(obj, file[, protocol])
　　序列化对象。并将结果数据流写入到文件对象中。参数protocol是序列化模式，默认值为0，表示以文本的形式序列化。protocol的值还可以是1或2，表示以二进制的形式序列化。
pickle.load(file)
　　反序列化对象。将文件中的数据解析为一个Python对象。

其中要注意的是，在load(file)的时候，要让python能够找到类的定义，否则会报错：
'''


def select_dummy_factors_from_file(file, header, dummy_columns, keep_top,
                                   replace_with, pickle_file):
    '''Memory constrained algorithm to select dummy factors from a large file
    对大文件使用内存约束算法选择dummy，一个真正的分布式的算法
    要输入文件路径、表头，要变成哑变量的列，保留的比例，
    '''

    dummy_dict = {}
    buffer_num = 0
    with open(file) as f:
        while True:
            buffer = f.readlines(
                1024000)  # Returns *at most* 1024000 bytes, maybe less
            if len(buffer) == 0:
                break
            else:
                buffer_list = [x.strip().split(",") for x in buffer]

                buffer_num += 1
                if ((buffer_num == 1) and (header is True)):
                    buffer_header = buffer_list[0]
                    buffer_starts = 1
                else:
                    buffer_starts = 0

                buffer_pdf = pd.DataFrame(buffer_list[buffer_starts:])
                if header is True:
                    buffer_pdf.columns = buffer_header

                dummy_dict_new = dummy_factors_counts(buffer_pdf,
                                                      dummy_columns)

                dummy_dict = cumsum_dicts(dummy_dict, dummy_dict_new)

    dummy_info = select_dummy_factors(dummy_dict, keep_top, replace_with,
                                      pickle_file)
    return (dummy_info)



if __name__ == "__main__":

    # User settings
    file = os.path.expanduser("~/data/airdelay_small.csv")
    header = True
    dummy_columns = [
        'Year', 'Month', 'DayOfWeek', 'UniqueCarrier', 'Origin', 'Dest'
    ]
    keep_top = [1, 1, 1, 0.8, 0.8, 0.8]
    replace_with = 'OTHERS'
    pickle_file = os.path.expanduser(
    "~/students/2020210977huojiaqi/spark/dummies/dummy_info_airdelay.pkl")

    dummy_info = select_dummy_factors_from_file(file, header, dummy_columns,
                                                keep_top, replace_with,
                                                pickle_file)



for i in dummy_info['factor_dropped'].keys():
    if len(dummy_info['factor_dropped'][i]) > 0:
        data = data.replace(dummy_info['factor_dropped'][i], 'OTHERS', i)




year = [int(i) for i in dummy_info['factor_selected']['Year']]
month = [int(i) for i in dummy_info['factor_selected']['Month']]
dayofweek = [int(i) for i in dummy_info['factor_selected']['DayOfWeek']]
uc = [i for i in dummy_info['factor_selected']['UniqueCarrier']]+['OTHERS']
ori = [i for i in dummy_info['factor_selected']['Origin']]+['OTHERS']
dest = [i for i in dummy_info['factor_selected']['Dest']]+['OTHERS']


dummies_year = [F.when(F.col("Year") == i, 1).otherwise(0).alias('year_'+str(i)) for i in year]
dummies_month = [F.when(F.col("Month") == i, 1).otherwise(0).alias('month_'+str(i)) for i in month]
dummies_dayofweek = [F.when(F.col("DayOfWeek") == i, 1).otherwise(0).alias('dayofweek_'+str(i)) for i in dayofweek]
dummies_uc = [F.when(F.col("UniqueCarrier") == i, 1).otherwise(0).alias('uc_'+i) for i in uc]
dummies_ori = [F.when(F.col("Origin") == i, 1).otherwise(0).alias('ori_'+i) for i in ori]
dummies_dest = [F.when(F.col("Dest") == i, 1).otherwise(0).alias('dest_'+i) for i in dest]

dummies = dummies_year+dummies_month+dummies_dayofweek+dummies_uc+dummies_ori+dummies_dest

data = data.select('Arrdelay','DayofMonth','DepTime','CRSDepTime','CRSArrTime','ActualElapsedTime','Distance',*dummies)
# shape (5423403, 155) 
print((data.count(), len(data.columns)))
# data.coalesce(1).write.option('header','true').csv('2020210977huojiaqi/airdelay_dummy_data')

