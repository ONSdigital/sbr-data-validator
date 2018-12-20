package action


import utils.HFileTestUtils
import dao.HBaseConnectionManager
import dao.hbase.HBaseDao
import dao.parquet.ParquetDao
import global.AppParams
import global.Configs.conf
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
import org.scalatest._
//import spark.extensions.rdd.HBaseDataReader._
import utils.Paths
import utils.data.existing.ExistingData

import scala.reflect.io.File


class AnalyserSpec extends HBaseConnectionManager with Paths with WordSpecLike with Matchers with BeforeAndAfterAll with ExistingData with HFileTestUtils{

  lazy val testDir = "newperiod"


  val appConfs = AppParams(
    (Array[String](
      "LINKS_201804", "ons", "l", existingLinksRecordHFiles,
      "LEU_201804", "ons", "d", existingLeusRecordHFiles,
      "ENT_201804", "ons", "d",existingEntRecordHFiles,
      "LOU_201804", "ons", "d",existingLousRecordHFiles,
      "REU_201804", "ons", "d",existingRusRecordHFiles,
      "201804",
      "HIVE DB NAME",
      "HIVE TABLE NAME",
      "HIVE SHORT TABLE NAME",
      payeFilePath,
      vatFilePath,
      "local",
      parquetPath
    )))


/*  override def beforeAll() = {
    implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    withHbaseConnection { implicit connection:Connection =>
      createRecords(appConfs)
    }
    spark.stop
  }*/


  "sbr-data-validator" should {
    "blah" in {
      implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("sbr-data-validator").getOrCreate()
      InputAnalyser.validate(appConfs)(spark)
      spark.stop()
      true shouldBe true
    }
  }

  def createRecords(appconf:AppParams)(implicit spark: SparkSession,connection:Connection) = {
    createHFiles(appconf)
    saveToHBase(appconf)
  }



  def createHFiles(appconf:AppParams)(implicit spark: SparkSession,connection:Connection) = {
    saveLinksToHFile(existingLinksForAddNewPeriodScenarion,appconf.HBASE_LINKS_COLUMN_FAMILY, appconf, existingLinksRecordHFiles)
    saveToHFile(existingLousForNewPeriodScenario,appconf.HBASE_LOCALUNITS_COLUMN_FAMILY, appconf, existingLousRecordHFiles)
    saveToHFile(existingRusForNewPeriodScenario,appconf.HBASE_REPORTINGUNITS_COLUMN_FAMILY, appconf, existingRusRecordHFiles)
    saveToHFile(existingLeusForNewPeriodScenario,appconf.HBASE_ENTERPRISE_COLUMN_FAMILY, appconf, existingLeusRecordHFiles)
    saveToHFile(existingEntsForNewPeriodScenario,appconf.HBASE_ENTERPRISE_COLUMN_FAMILY, appconf, existingEntRecordHFiles)
  }

  def saveToHBase(appconf:AppParams)(implicit spark: SparkSession,con:Connection) = {
    HBaseDao.truncateTables(con,appconf)
    HBaseDao.loadLinksHFile(con,appconf)
    HBaseDao.loadEnterprisesHFile(con,appconf)
    HBaseDao.loadLousHFile(con,appconf)
    HBaseDao.loadLeusHFile(con,appconf)
    HBaseDao.loadRusHFile(con,appconf)
  }

}
