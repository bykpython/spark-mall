package com.bigdata.sparkmall.common.utils

import java.text.SimpleDateFormat
import java.util.Date

object DateUtil {

  def getStringByTimestamp(ts:Long, f:String): String ={
    val date = new Date(ts)
    getStringByDate(date, f)
  }

  def getStringByDate(d :Date, f:String): String = {

    val format = new SimpleDateFormat(f)
    format.format(d)
  }

}
