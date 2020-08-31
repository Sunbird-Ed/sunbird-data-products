package org.sunbird.analytics.job.report

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

class BaseCourseReport {

  def loadData(spark: SparkSession, settings: Map[String, String], url: String, schema: StructType): DataFrame = {
    if (schema.nonEmpty) {
      spark.read.schema(schema).format(url).options(settings).load()
    }
    else {
      spark.read.format(url).options(settings).load()
    }
  }

}
