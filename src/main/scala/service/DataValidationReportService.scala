package service

import action.InputAnalyser
import dao.HBaseConnectionManager
import global.AppParams
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import spark.SparkSessionManager

trait DataValidationReportService extends HBaseConnectionManager with SparkSessionManager{

      def validate(appconf:AppParams) = withSpark(appconf) { implicit ss: SparkSession =>
        withHbaseConnection { implicit con: Connection =>

          InputAnalyser.getData(appconf)
        }
      }
}
