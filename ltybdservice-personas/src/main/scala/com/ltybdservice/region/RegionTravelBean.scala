package com.ltybdservice.region

import com.ltybdservice.config.FilterParam
import com.ltybdservice.{Region, User, Validate}


object RegionTravelBean {
  lazy val minInterval=FilterParam.param.getMinInterval
  lazy val maxInterval=FilterParam.param.getMaxInterval

  /**
    *
    * @param phoneModel
    * @param cityCode
    * @param timeType
    * 按用户、周规律分组的key
    */
  case class UserWeekGroupKey(phoneModel:String, cityCode:String, timeType:String)

  /**
    *
    * @param originRegion
    * @param startDaySeconds
    * @param destRegion
    * @param endDaySeconds
    * @param interval
    * @param validTravelCount
    */
  case class Travel(originRegion: Region, startDaySeconds: Long, destRegion: Region, endDaySeconds: Long, interval: Long, validTravelCount: Long)

  /**
    *
    * @param user
    * @param timeType
    * @param travel
    */
  case class UserTimeTravel(user:User, timeType:String,travel:Travel) extends Validate {
    override def validate() = {
      if((!user.validate())||timeType.isEmpty||travel.interval<minInterval||travel.interval>maxInterval){
        false
      }else{
        true
      }
    }
  }

  /**
    *
    * @param userId
    * @param cityCode
    * @param originRegion
    * @param startDayTime
    * @param destRegion
    * @param endDayTime
    * @param transfer
    */
  case class UserTimeRegionTransferTravel(userId: String, phoneModel: String, cityCode: String, timeType:String, originRegion: String, startDayTime: String, destRegion: String, endDayTime: String, transfer: Boolean) extends Validate {
    override def validate() = {
      if((userId.isEmpty&&phoneModel.isEmpty)||cityCode.isEmpty||timeType.isEmpty){
        false
      }else{
        true
      }
    }
  }
}