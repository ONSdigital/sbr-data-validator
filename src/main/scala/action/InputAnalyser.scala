package action

import dao.hbase.HBaseDao
import global.{AppParams, Configs}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spark.RddLogging
import spark.extensions.df._
import model._



case class DataReport(entCount:Long, lusCount:Long, losCount:Long, childlessEntErns:Seq[String], entsWithBrokenkeys:RDD[(String, String)] ,lusOrphans:RDD[(String, (String, String))] , losOrphans:RDD[(String,(String,String))])

object InputAnalyser extends RddLogging{

  def getDfFormatData(appconf:AppParams)(implicit spark: SparkSession):Unit =  {

      val links = getLinksDF(appconf)

      val ents = getEntsDF(appconf)

      val lus = getLegalUnitDF(appconf)

      val lous = getLocallUnitDF(appconf)

      val rus = getReportingUnitDF(appconf)

      val entLinks = getEnterpriseUnitLinksDF(appconf)

      val leuLinks = getLegalUnitLinksDF(appconf)

      val louLinks = getLocalUnitLinksDF(appconf)

      val ruLinks = getReportingUnitLinksDF(appconf)

      val vatLinks = getVatLinksDF(appconf)

      val payeLinks = getPayeLinksDF(appconf)

      val chLinks = getChLinksDF(appconf)
  }

  def getEnterpriseUnitLinksDF(appconf:AppParams)(implicit spark:SparkSession) = {
    val entLinksRows:RDD[Row] = HBaseDao.readLinksWithKeyPrefixFilter(Configs.conf,appconf, "ENT~")map(_.toEntLinkRow)
    spark.createDataFrame(entLinksRows, linksEntRowSchema)
  }
  def getLegalUnitLinksDF(appconf:AppParams)(implicit spark:SparkSession) = {
    val leuLinksRows:RDD[Row] = HBaseDao.readLinksWithKeyPrefixFilter(Configs.conf,appconf, "LEU~").map(_.toLeuLinksRow)
    spark.createDataFrame(leuLinksRows, linksLeuRowSchema)
  }

  def getReportingUnitLinksDF(appconf:AppParams)(implicit spark:SparkSession) = {
    val ruLinksRows:RDD[Row] = HBaseDao.readLinksWithKeyPrefixFilter(Configs.conf,appconf, "REU~").map(_.toRuLinksRow)
    spark.createDataFrame(ruLinksRows, linksRuRowSchema)
  }
  def getLocalUnitLinksDF(appconf:AppParams)(implicit spark:SparkSession) = {
    val louLinksRows:RDD[Row] = HBaseDao.readLinksWithKeyPrefixFilter(Configs.conf,appconf, "LOU~").map(_.toLouLinksRow)
    spark.createDataFrame(louLinksRows, linksLouRowSchema)
  }

  def getVatLinksDF(appconf:AppParams)(implicit spark:SparkSession) = {
    val vatLinksRows:RDD[Row] = HBaseDao.readLinksWithKeyPrefixFilter(Configs.conf,appconf, "VAT~").map(_.toVatLinksRow)
    spark.createDataFrame(vatLinksRows, linksVatRowSchema)
  }

  def getPayeLinksDF(appconf:AppParams)(implicit spark:SparkSession) = {
    val payeLinksRows:RDD[Row] = HBaseDao.readLinksWithKeyPrefixFilter(Configs.conf,appconf, "PAYE~").map(_.toPayeLinksRow)
    spark.createDataFrame(payeLinksRows, linksPayeRowSchema)
  }
  def getChLinksDF(appconf:AppParams)(implicit spark:SparkSession) = {
    val chLinksRows:RDD[Row] = HBaseDao.readLinksWithKeyPrefixFilter(Configs.conf,appconf, "CH~").map(_.toChLinksRow)
    spark.createDataFrame(chLinksRows, linksChRowSchema)
  }


  def getLinksDF(appconf:AppParams)(implicit spark: SparkSession): DataFrame =  {
    val ruRows:RDD[Row] = HBaseDao.readTable(appconf,Configs.conf, HBaseDao.linksTableName(appconf)).map(_.toLouRow)
    spark.createDataFrame(ruRows, ruRowSchema)
  }

  def getReportingUnitDF(appconf:AppParams)(implicit spark: SparkSession): DataFrame =  {
    val ruRows:RDD[Row] = HBaseDao.readTable(appconf,Configs.conf, HBaseDao.rusTableName(appconf)).map(_.toLouRow)
    spark.createDataFrame(ruRows, ruRowSchema)
  }

  def getLocallUnitDF(appconf:AppParams)(implicit spark: SparkSession): DataFrame =  {
    val tuRows:RDD[Row] = HBaseDao.readTable(appconf,Configs.conf, HBaseDao.lousTableName(appconf)).map(_.toRuRow)
    spark.createDataFrame(tuRows, ruRowSchema)
  }


  def getLegalUnitDF(appconf:AppParams)(implicit spark: SparkSession): DataFrame =  {
    val leuRows:RDD[Row] = HBaseDao.readTable(appconf,Configs.conf, HBaseDao.leusTableName(appconf)).map(_.toLeuLinksRow)
    spark.createDataFrame(leuRows, linksLeuRowSchema)
  }

  def getEntsDF(appconf:AppParams)(implicit spark: SparkSession): DataFrame =  {
    val entsRows:RDD[Row] = HBaseDao.readTable(appconf, Configs.conf,HBaseDao.entsTableName(appconf)).map(_.toEntRow)
    spark.createDataFrame(entsRows, entRowSchema)
  }

  def getData(appconf:AppParams)(implicit spark: SparkSession):DataReport =  {


    val links = getLinksDF(appconf).cache()

    val ents = getEntsDF(appconf).cache()

    val lus = getLegalUnitDF(appconf).cache()

    val lous = getLocallUnitDF(appconf).cache()

    val rus = getReportingUnitDF(appconf).cache()

    val entLinks = getEnterpriseUnitLinksDF(appconf).cache()

    val leuLinks = getLegalUnitLinksDF(appconf).cache()

    val louLinks = getLocalUnitLinksDF(appconf).cache()

    val ruLinks = getReportingUnitLinksDF(appconf).cache()

    val vatLinks = getVatLinksDF(appconf).cache()

    val payeLinks = getPayeLinksDF(appconf).cache()

    val chLinks = getChLinksDF(appconf).cache()


    links.unpersist()
    ents.unpersist()
    lus.unpersist()
    lous.unpersist()
    rus.unpersist()
    entLinks.unpersist()
    leuLinks.unpersist()

    ???
  }



  def getRepartionedRdd[T](rdd:RDD[T]) = {
    val noOfPartiions = rdd.getNumPartitions
    rdd.repartition(noOfPartiions)
    rdd
  }

  def getChildlessEnts(entErns:RDD[String],luErns:RDD[String],loErns:RDD[String]) = {
    val luLessEnts = entErns.subtract(luErns)
    val loLessEnts = entErns.subtract(loErns)
    loLessEnts.intersection(luLessEnts)
  }

  def getOrphanLus(lus:RDD[HFileRow], orphanLuErns:RDD[String])(implicit spark: SparkSession) = {
    val numberOfPartitions = lus.getNumPartitions
    val orphanLuErnsRows: RDD[(String,String)] = getRepartionedRdd(orphanLuErns.map(ern => (ern,ern))) //create tuple of 2 duplicate erns
    val luRows: RDD[(String, (String, String))] = getRepartionedRdd(lus.map(row => (row.getValueOrStr("p_ENT"),(row.key.split("~").head , row.key))) )//tuple(ern,(ubrn,row key))
    val joined: RDD[(String, ((String, String), Option[String]))] = luRows.leftOuterJoin(orphanLuErnsRows)
    val orphanLuUbrn = joined.collect { case (ern, ((ubrn,key), None)) => (ern, (ubrn,key)) }
    orphanLuUbrn.coalesce(numberOfPartitions)

  }

  def getOrphanLos(los:RDD[HFileRow], orphanLoErns:RDD[String])(implicit spark: SparkSession) = {
    val numberOfPartitions = los.getNumPartitions
    val orphanLoErnsRows: RDD[(String,String)] = getRepartionedRdd(orphanLoErns.map(ern => (ern,ern)))
    val loRows: RDD[(String, (String, String))] = getRepartionedRdd(los.map(row => (row.getValueOrStr("ern"),(row.key.split("~").last , row.key))))
    val joined: RDD[(String, ((String, String), Option[String]))] = loRows.leftOuterJoin(orphanLoErnsRows)
    val orphanLoLurn = joined.collect { case (ern, ((lurn,key), None)) => (ern, (lurn,key)) }
    orphanLoLurn.coalesce(numberOfPartitions)

  }



}

