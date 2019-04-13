package com.bigdata.sparkmall.common.utils

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.util.{Properties, ResourceBundle}

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.configuration2.PropertiesConfiguration
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters

object ConfigurationUtil {

  private val rb = ResourceBundle.getBundle("config")

  def getValueByKeyFromConfig(config: String, key: String): String ={
    ResourceBundle.getBundle(config).getString(key)
  }

  def getValueByKey(key: String): String ={

    val properties = new Properties()

    val stream: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("config.properties")

    val reader = new BufferedReader(new InputStreamReader(stream))

    properties.load(reader)

    val str = properties.getProperty(key)

    str
  }

  def getCondValue(key: String): String ={
    val str: String = ResourceBundle.getBundle("condition").getString("condition.params.json")

    val jsonObject: JSONObject = JSON.parseObject(str)

    jsonObject.getString(key)
  }
}
