package com.cloudera.forecast

import java.io.{BufferedReader, InputStreamReader}
import java.net.{URL, URLConnection}
import java.util._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ArrayNode
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, RowFactory}

class DarkSkyForecasts {

  def getWeatherForcastsFor(in_lat: String, in_long: String): scala.collection.mutable.LinkedList[Row] = {
    val urlStr = "https://api.darksky.net/forecast/a006ff894f118cf45f98922527bd315b/".concat(in_lat).concat(",").concat(in_long)
    val darkSky: URL = new URL(urlStr)
    val darkSkyConnection: URLConnection = darkSky.openConnection
    val resultReader: BufferedReader = new BufferedReader(new InputStreamReader(darkSkyConnection.getInputStream))

# comment

    var api_result: String = null
    var forecastRows = new scala.collection.mutable.LinkedList[Row]
    while ( { api_result = resultReader.readLine; api_result } != null) {
      val objectMapper: ObjectMapper = new ObjectMapper
      val rootNode: JsonNode = objectMapper.readTree(api_result)
      val latitude: Double = rootNode.get("latitude").asDouble
      val longitude: Double = rootNode.get("longitude").asDouble
      val timezone: String = rootNode.get("timezone").asText
      val tz_offset: Integer = rootNode.get("offset").asInt
      val hourlyNode: JsonNode = rootNode.get("hourly")
      val dataNodes: ArrayNode = hourlyNode.get("data").asInstanceOf[ArrayNode]
      val dataNodeElements: Iterator[JsonNode] = dataNodes.iterator
      while (dataNodeElements.hasNext) {
        val dataNode: JsonNode = dataNodeElements.next
        val time = dataNode.get("time").asInt
        val precipIntensity: Double = dataNode.get("precipIntensity").asDouble
        val precipProbability: Double = dataNode.get("precipProbability").asDouble
        val temperature: Double = dataNode.get("temperature").asDouble
        val apparentTemperature: Double = dataNode.get("apparentTemperature").asDouble

        forecastRows = forecastRows :+ RowFactory.create(latitude: java.lang.Double, longitude: java.lang.Double,
          timezone: java.lang.String, tz_offset: java.lang.Integer, time: java.lang.Integer,
          precipIntensity: java.lang.Double, precipProbability: java.lang.Double,
          temperature: java.lang.Double, apparentTemperature: java.lang.Double)
      }
    }
    resultReader.close()
    forecastRows
  }

  def saveWeatherForcasts(hc: org.apache.spark.sql.hive.HiveContext,
                          forecastRowsRDD: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row],
                          in_partition: String) = {

    val forecastSchema = StructType(Array(
      StructField("latitude", DoubleType, nullable = true),
      StructField("longitude", DoubleType, nullable = true),
      StructField("timezone", StringType, nullable = true),
      StructField("tz_offset", IntegerType, nullable = true),
      StructField("forecast_time", IntegerType, nullable = true),
      StructField("precip_intensity", DoubleType, nullable = true),
      StructField("precip_probability", DoubleType, nullable = true),
      StructField("temperature", DoubleType, nullable = true),
      StructField("apparent_temperature", DoubleType, nullable = true)
    ))

    val newData = hc.createDataFrame(forecastRowsRDD, forecastSchema)
    val path: String = "/incoming/weather/forecast/" + in_partition
    newData.write.parquet(path)
  }

  def getResturantLatLongs(hc: org.apache.spark.sql.hive.HiveContext) : org.apache.spark.sql.DataFrame  ={
    val rest_ids =  "('001.005.0043','001.005.0078','001.005.0110','001.005.0123','001.005.0234','001.005.0241','001.005.0246','001.005.0249','001.005.0364','001.005.0471','001.005.0496','001.005.0505','001.005.0522','001.005.0537','001.005.0540','001.005.0567','001.005.0616','001.005.0627','001.005.0637','001.005.0655','001.005.0664','001.005.0672','001.005.0695','001.005.0704','001.005.0723','001.005.0788','001.005.0846','001.005.0868','001.005.0873','001.005.0891','001.005.0914','001.005.0922','001.005.0952','001.005.0977','001.005.1062','001.005.1074','001.005.1076','001.005.1096','001.005.1102','001.005.1107','001.005.1108','001.005.1109','001.005.1136','001.005.1141','001.005.1145','001.005.1152','001.005.1158','001.005.1163','001.005.1164','001.005.1174','001.005.1183','001.005.1191','001.005.1200','001.005.1226','001.005.1246','001.005.1254','001.005.1281','001.005.1305','001.005.1317','001.005.1334','001.005.1351','001.005.1369','001.005.1373','001.005.1414','001.005.1417','001.005.1465','001.005.1476','001.005.1536','001.005.1546','001.005.1550','001.025.0052','001.025.0139','001.025.0166','001.025.0179','001.025.0184','001.025.0191','001.025.0195','001.025.0197','001.025.0199','001.025.0207')"
    val select_stmt = "SELECT coordinate_latitude, coordinate_longitude FROM ref_analysis.rest_loc WHERE row_current_flag = 'Y' AND restaurant_id IN ".concat(rest_ids)
    val df = hc.sql(select_stmt)
    df
  }

  def assembleWeatherForecastsForAllRestaurants(hc: org.apache.spark.sql.hive.HiveContext, partition: String) = {
    val df = getResturantLatLongs(hc)
    var forecastRows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = null
    forecastRows  = df.flatMap(r => getWeatherForcastsFor(r.getString(0), r.getString(1)))
    forecastRows.collect
    saveWeatherForcasts(hc, forecastRows, partition)
  }

  def main(args: Array[String]): Unit = {
    var partition = new String
    if(args.length < 1) {
      val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd")
      partition = dateFormat.format(new Date())
    } else {
      partition = args(0) // partition can be set to something like yyyymmdd (i.e. 20160928)
    }

    val sc = new SparkContext()
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    hc.setConf("spark.sql.parquet.compression.codec", "uncompressed")
    val dsf = new DarkSkyForecasts()
    dsf.assembleWeatherForecastsForAllRestaurants(hc, partition)
  }

}
