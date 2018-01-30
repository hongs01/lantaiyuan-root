package com.ltybdservice.region

import com.ltybdservice.config.FilterParam
import com.ltybdservice.region.RegionTravelBean._
import com.ltybdservice._
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable

/**
  * 聚合求用户行程
  */
object RegionTravelAgg {
  lazy val minUserInfoSize= FilterParam.param.getMinUserInfoSize
  lazy val odInfoSize= FilterParam.param.getOdInfoSize
  /**
    *
    * @param spark
    * @param userRealTimeRegion
    *
    */
  def getTransferRegionTravel(spark: SparkSession, userRealTimeRegion: Dataset[UserRealTimeRegion]): Dataset[UserTimeRegionTransferTravel] = {
    import spark.implicits._
    //假设数据按星期呈现规律，基于此假设进行计算，每个工作日上午去上班，下午回家，周五下班可能去其他地方，周末上午去一个地方，下午回来
    val userTimeGroup = userRealTimeRegion.groupByKey(userRealTimeRegion => {
      val user = userRealTimeRegion.userRealTime.user
      UserWeekGroupKey(user.phoneModel, user.cityCode, userRealTimeRegion.timeType)
    })
    /**
      * 对分组后的数据进行任意聚合，得到分组时间内的行程信息，在分组时间内只有一个行程
      */
    val regionTravels = userTimeGroup.mapGroups(RegionTravelAgg.regionAggregator(_,_)).filter(_.validate())
    val transferTravels = regionTravels.map(regionTravel => {
      val user=regionTravel.user
      val travel=regionTravel.travel
      val startHourMinutesSeconds = Transform.daySeconds2HourMinutesSeconds(travel.startDaySeconds)
      val endHourMinutesSeconds = Transform.daySeconds2HourMinutesSeconds(travel.endDaySeconds)
//      val originLocation = regionTravel.originRegion.location
//      val destLocation = regionTravel.destRegion.location
//      val transfer = Transform.transfer(originLocation.longitude, originLocation.latitude, destLocation.longitude, destLocation.latitude, regionTravel.cityCode)
      UserTimeRegionTransferTravel(user.userId,user.phoneModel, user.cityCode,regionTravel.timeType, travel.originRegion.toString(), startHourMinutesSeconds, travel.destRegion.toString(), endHourMinutesSeconds, true)
    }).filter(_.validate())
    transferTravels
  }

  /**
    *
    * @param key
    * @param userRealTimeRegionIt
    * @return 聚合分组时间内的用户行程，分组时间内只有一个行程，过滤不对的行程
    */
  def regionAggregator(key: UserWeekGroupKey, userRealTimeRegionIt: Iterator[UserRealTimeRegion]): UserTimeTravel = {
    val zeroUser=User("","", "")
    val zeroRegion=Region(Location(0,0),"")
    val zeroTravel=Travel(zeroRegion, 0, zeroRegion, 0, 0, 0)
    val zeroUserTimeTravel = UserTimeTravel(zeroUser,"",zeroTravel)
    val zeroMutableHashMap = mutable.HashMap[Region, Long]()
    // 如果数据太少则返回会被过滤的值,注意Iterator要转为list使用  Iterator是懒序列，只有在用的时候再会计算，通过toList，将它全部计算出来
    val userRealTimeRegionList = userRealTimeRegionIt.toList
    if (userRealTimeRegionList.size < minUserInfoSize) {
      zeroUserTimeTravel
    } else {
      // 聚合统计用户在每个区域上传数据的次数
      val rawRegionLongMap = userRealTimeRegionList.foldLeft(zeroMutableHashMap)(RegionTravelAgg.foldLeftRegionCount(_,_))
      //如果始终聚合在一个上，说明用户没有行程，返回会被过滤的值
      if (rawRegionLongMap.size < 2) {
        zeroUserTimeTravel
      } else{
        // 按区域次数降序用户区域信息
        val sortRegionLongList = rawRegionLongMap.toList.sortWith(_._2 > _._2)
        if(sortRegionLongList(0)._2<odInfoSize||sortRegionLongList(1)._2<odInfoSize){
          zeroUserTimeTravel
        }else{
          //取区域次数最多的两个区域，如果用户行程是有规律的，次数最多的两个区域最有可能是od点
          val odRegion = (sortRegionLongList(0)._1, sortRegionLongList(1)._1)
          //过滤非od点用户信息，只保留用户在od点的信息，注意在此之后只有od点的用户信息了
          val odUserRealTimeRegionIterator = userRealTimeRegionList.filter(u => (u.region == odRegion._1 || u.region == odRegion._2))
          //按日期（天）对用户数据分组
          val dayUser = odUserRealTimeRegionIterator.groupBy(userRegion => new java.sql.Date(userRegion.userRealTime.currentTime.getTime).toString)
          //聚合按天分组后的行程信息
          dayUser.foldLeft(zeroUserTimeTravel)(RegionTravelAgg.foldLeftRegionTravel(_,_))
        }
      }
    }
  }

  /**
    *
    * @param regionCount
    * @param userRealTimeRegion
    * @return 返回用户在每个区域的次数
    */
  def foldLeftRegionCount(regionCount: mutable.HashMap[Region, Long], userRealTimeRegion: UserRealTimeRegion): mutable.HashMap[Region, Long] = {
    val count = regionCount.get(userRealTimeRegion.region)
    if (count != None) {
      regionCount.put(userRealTimeRegion.region, count.get + 1)
      regionCount
    } else {
      regionCount.put(userRealTimeRegion.region, 1)
      regionCount
    }
  }


  /**
    *
    * @param userTimeTravel   迭代结果
    * @param userRealTimeRegionList 用户信息按天分组的数据，包含在出发地，目的地两个区域的信息
    * @return 聚合按天分组后的行程信息
    */
  def foldLeftRegionTravel(userTimeTravel: UserTimeTravel, userRealTimeRegionList: (String, List[UserRealTimeRegion])): UserTimeTravel = {
    val zeroUser=User("","", "")
    val zeroRegion=Region(Location(0,0),"")
    val zeroTravel=Travel(zeroRegion, 0, zeroRegion, 0, 0, 0)
    val zeroUserTimeTravel = UserTimeTravel(zeroUser,"",zeroTravel )
    //对每天的用户信息按时间从早到晚进行排序
    val newDaySortUserRealTimeRegion = userRealTimeRegionList._2.sortWith(_.userRealTime.currentTime.getTime < _.userRealTime.currentTime.getTime)
    //可变列表，用于存储不定个数的行程
    val newTravelBuffer = new mutable.ListBuffer[Travel]()
    val it = newDaySortUserRealTimeRegion.iterator
    val userRealTimeRegion = it.next()
    //设置起始地点信息
    var originRegion: Region = userRealTimeRegion.region
    var startDaySeconds: Long = Transform.timestamp2DaySeconds(userRealTimeRegion.userRealTime.currentTime)
    while (it.hasNext) {
      val itUserRealTimeRegion = it.next()
      if (originRegion == itUserRealTimeRegion.region) {
        //不断更新出发时间
        startDaySeconds = Transform.timestamp2DaySeconds(itUserRealTimeRegion.userRealTime.currentTime)
      }
      else {
        /**
          * originRegion,startDaySeconds都是可变的,将行程添加到当天行程列表
          * 如果新的区域名与上一个记录的区域名不一样，说明到了目的地，确定一趟行程
          */
        val endDaySeconds = Transform.timestamp2DaySeconds(itUserRealTimeRegion.userRealTime.currentTime)
        val userRealTime = itUserRealTimeRegion.userRealTime
        val travel=Travel(originRegion, startDaySeconds, itUserRealTimeRegion.region, endDaySeconds, endDaySeconds - startDaySeconds, 0)
        newTravelBuffer.append(travel)
        //保存完已知行程后，将新的用户记录作为起始地点
        originRegion = itUserRealTimeRegion.region
        startDaySeconds = Transform.timestamp2DaySeconds(userRealTime.currentTime)
      }
    }
    //没有行程
    if(newTravelBuffer.isEmpty){
      zeroUserTimeTravel
    }else{
      //取行程最长的记录作为行程
      val maxTravel = newTravelBuffer.maxBy(_.interval)
      //求平均时间，如果是第一条数据则直接返回所得行程
      if (!userTimeTravel.validate()) {
        UserTimeTravel(userRealTimeRegion.userRealTime.user,userRealTimeRegion.timeType,maxTravel)
      } else if (userTimeTravel.travel.originRegion != maxTravel.originRegion) {
      //如果相邻两天方向不一致，返回zero值
      zeroUserTimeTravel
    } else {
        val travel=userTimeTravel.travel
        val avgStartDaySeconds = (travel.startDaySeconds * travel.validTravelCount + maxTravel.startDaySeconds) / (travel.validTravelCount + 1)
        val avgEndDaySeconds = (travel.endDaySeconds * travel.validTravelCount + maxTravel.endDaySeconds) / (travel.validTravelCount + 1)
        val newTravel=Travel(travel.originRegion, avgStartDaySeconds, travel.destRegion, avgEndDaySeconds, avgEndDaySeconds - avgStartDaySeconds, travel.validTravelCount + 1)
        UserTimeTravel(userTimeTravel.user,userTimeTravel.timeType,newTravel)
      }
    }
  }
}
