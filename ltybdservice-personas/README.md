#本工程是蓝泰源大数据用户画像工程
##1、数据


1、用户的gps坐标信息，存储于hive(dc.dwd_user_gps_tmp)

userId|longitude|latitude|phoneModel|currentTime|cityCode|geocode6|geocode7|geocode8
:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:
string|string|string|string|string|string|string|string|string|
4036|113.938740|22.561402|860443038952215|2017-11-13 19:00:43|130400|ws102e|ws102ep|ws102ept

注意userId可能为空

##2、分析
1、OD分析

od是一个区域，该区域可以是一个站点，也可以是一个geohash编码所代表的区域
将原始数据整理成如下格式

userId|longitude|latitude|phoneModel|currentTime|cityCode|region
:---:|:---:|:---:|:---:|:---:|:---:|:---:
string|string|string|string|string|string|string|
4036|113.938740|22.561402|860443038952215|2017-11-13 19:00:43|130400|ws102e

然后统计用户在不同时间类型（工作日上午，工作日下午，周五下午，周末上午，周末下午），在不同区域的信息量，
取信息量最大的两个作为候选od点，滤除非od点的用户坐标信息，将候选信息按天分组，在每一天（其实只有半天）内按时间先后排序，
最开始的区域是其实区域，即o点，检查区域位置是否发生改变，发生改变，则说明到达终点，即d点，可以得到一趟行程
在某种类型的半天内可能有多次反复，取最长的一次作为当天行程，取某种时间类型的所有起始时间的平均值作为该类型的起始时间，
取该时间类型的所有终止时间平均作为该类型的终止时间。得到每个用户的行程，剔除明显不对的数据，然后判断该用户是否需要换乘。

输出结果表如下：

userId|phoneModel|cityCode|timeType|originRegion|startDayTime|destRegion|endDayTime|transfer
:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:
text|text|text|text|text|text|text|text|bit(1)




