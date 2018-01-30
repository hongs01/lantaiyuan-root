package com.ltybdservice

import com.ltybdservice.geohashutil.GeoHash
import com.ltybdservice.redisutil.RedisConf
import redis.clients.jedis.GeoUnit
import redis.clients.jedis.params.geo.GeoRadiusParam
trait Validate {
  def validate(): Boolean={
    true
  }
}
case class User(userId: String, phoneModel:String, cityCode: String)extends Validate{
  override def validate() = {
    if((userId.isEmpty&&(phoneModel.isEmpty))||cityCode.isEmpty){
      false
    }else{
      true
    }
  }
}
case class RawUserRealTime(userId: String, phoneModel:String, cityCode: String, longitude: Double, latitude: Double, currentTime: java.sql.Timestamp, region:String) extends Validate{
  override def validate() = {
    if((userId.isEmpty&&(phoneModel.isEmpty))||cityCode.isEmpty){
      false
    }else{
      true
    }
  }
}
case  class Region(location: Location,string:String) extends Validate{
  override def validate() = {
    if(!location.validate()){
      false
    }else{
      true
    }
  }
  override def toString(): String = {
    string
  }
}
case class Location(longitude: Double, latitude: Double) extends Validate{
  override def validate() = {
    if(longitude.toString.isEmpty||latitude.toString.isEmpty){
      false
    }else{
      true
    }
  }
}
case class Station(stationId: String, cityCode: String, location: Location)extends Validate{
  override def validate() = {
    if(stationId.isEmpty||cityCode.isEmpty||(!location.validate())){
      false
    }else{
      true
    }
  }
  def getLocation(): Location = {
    location
  }
}
object Station{
  /**
    *
    * @param userRealTime
    * @param km
    * @return
    * 可能在附近范围内找不到站点，因此返回一个Option
    */
  def nearStation(userRealTime:UserRealTime, km: Double): Option[Station] = {
    //TODO 可能是个bug，可能存在序列化的问题
    val jc = RedisConf.jc
    val listRes = jc.georadius(userRealTime.user.cityCode, userRealTime.location.longitude, userRealTime.location.latitude, km, GeoUnit.KM, GeoRadiusParam.geoRadiusParam.sortAscending.count(1).withCoord())
    if (listRes.isEmpty) {
      None
    }
    else {
      val coordinate = listRes.get(0).getCoordinate()
      val stationId = listRes.get(0).getMemberByString().split("-")(0)
      Some(Station(stationId, userRealTime.user.cityCode, Location(coordinate.getLongitude, coordinate.getLatitude)))
    }
  }
}
case class District(district: String) extends  Validate{
  override def validate() = {
    if(district.isEmpty){
      false
    }else{
      true
    }
  }
   def getLocation(): Location = {
    val geoHash=GeoHash.fromGeohashString(district)
    val point=geoHash.getBoundingBoxCenterPoint
    Location(point.getLongitude,point.getLatitude)
  }
}
case class UserRealTime(user: User, location: Location, currentTime: java.sql.Timestamp) extends Validate{
  override def validate() = {
    if((!user.validate())||(!location.validate())){
      false
    }else{
      true
    }
  }
}
case class UserRealTimeRegion(userRealTime:UserRealTime, timeType:String, region:Region) extends Validate{
  override def validate() = {
    if((!userRealTime.validate())||timeType.isEmpty||(!region.validate())){
      false
    }else{
      true
    }
  }
}





