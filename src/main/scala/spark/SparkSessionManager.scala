package spark

import global.AppParams
import org.apache.spark.sql.SparkSession



trait  SparkSessionManager {

  def withSpark(appconf:AppParams)(doWithinSparkSession: SparkSession => Unit) = {

    implicit val spark: SparkSession = {
      if (appconf.ENV == "cluster") SparkSession.builder().appName("sbr-data-validator").enableHiveSupport().getOrCreate()
      else SparkSession.builder().master("local[8]").appName("sbr-data-validator").getOrCreate()
    }

    doWithinSparkSession(spark)

    spark.stop()

  }
}
