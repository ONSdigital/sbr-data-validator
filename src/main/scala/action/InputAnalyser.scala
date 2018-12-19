package action

import dao.hbase.HBaseDao
import global.{AppParams, Configs}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import spark.RddLogging
import spark.extensions.df._
import model._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema



case class DataReport(entCount:Long, lusCount:Long, losCount:Long, childlessEntErns:Seq[String], entsWithBrokenkeys:RDD[(String, String)] ,lusOrphans:RDD[(String, (String, String))] , losOrphans:RDD[(String,(String,String))])

object InputAnalyser extends RddLogging{


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

  def getReportingUnitDF(appconf:AppParams)(implicit spark: SparkSession): DataFrame =  {
    val ruRows:RDD[Row] = HBaseDao.readTable(appconf,Configs.conf, HBaseDao.rusTableName(appconf)).map(_.toRuRow)
    spark.createDataFrame(ruRows, ruRowSchema)
  }

  def getLocallUnitDF(appconf:AppParams)(implicit spark: SparkSession): DataFrame =  {
    val tuRows:RDD[Row] = HBaseDao.readTable(appconf,Configs.conf, HBaseDao.lousTableName(appconf)).map(_.toLouRow)
    spark.createDataFrame(tuRows, louRowSchema)
  }


  def getLegalUnitDF(appconf:AppParams)(implicit spark: SparkSession): DataFrame =  {
    val leuRows:RDD[Row] = HBaseDao.readTable(appconf,Configs.conf, HBaseDao.leusTableName(appconf)).map(_.toLeuRow)
    spark.createDataFrame(leuRows, linksLeuRowSchema)
  }

  def getEntsDF(appconf:AppParams)(implicit spark: SparkSession): DataFrame =  {
    val entsRows:RDD[Row] = HBaseDao.readTable(appconf, Configs.conf,HBaseDao.entsTableName(appconf)).map(_.toEntRow)
    spark.createDataFrame(entsRows, entRowSchema)
  }

  def getData(appconf:AppParams)(implicit spark: SparkSession):Unit =  {

    import org.apache.spark.sql.functions._

    val ents = getEntsDF(appconf).cache()
    val entLinks = getEnterpriseUnitLinksDF(appconf).cache()

    val leus = getLegalUnitDF(appconf).cache()
    val leuLinks = getLegalUnitLinksDF(appconf).cache()

    val lous = getLocallUnitDF(appconf).cache()
    val louLinks = getLocalUnitLinksDF(appconf).cache()

    val rus = getReportingUnitDF(appconf).cache()
    val ruLinks = getReportingUnitLinksDF(appconf).cache()

    val vatLinks = getVatLinksDF(appconf).cache()

    val payeLinks = getPayeLinksDF(appconf).cache()

    val chLinks = getChLinksDF(appconf).cache()

    /**
      * +-------------------+-------+--------------+--------------+-----------------+---------------------+---------------------+-------+---------+-------------+--------+
      * | primary unit type | table | p.u.id name  | p.u.id value | fk to s.u. name |   fk to s.u. value  | secondary unit type | table | period  | what's wrong| message|
      * +-------------------+-------+--------------+--------------+-----------------+---------------------+---------------------+-------+---------+-------------+--------+
      * */
    def toReportEntry(df:DataFrame,unit:String,puTable:String,puIdname:String, fkName:String, secUnit:String,suTable:String, period:String,problemType:String, message:String)(implicit spark:SparkSession) = {
      import spark.implicits._
      spark.createDataFrame(
      df.rdd.map ( row => Row(
                          unit,
                          puTable,
                          puIdname,
                          row.getValueOrNull(puIdname),
                          fkName,
                          row.getValueOrNull(fkName),
                          secUnit,
                          suTable,
                          period,
                          problemType,
                          message
                  )), reportRowSchema)
    }
    /**
      * Local Units
      * */
    //lous without parent Reporting Unit on Unit tables level
    val ru_less_lous = lous.join(rus,Seq("rurn"), "left_anti")
    val ru_less_lous_report = toReportEntry(ru_less_lous,"Local Unit", "LOU","lurn","rurn","Reporting Unit","REU",appconf.PERIOD,"CHILDLESS", "Local Unit: No REU children found in REU table")
    //lous without ru on links level
    val ru_less_lous_links = louLinks.join(ruLinks,Seq("rurn"), "left_anti")
    val ru_less_lous_links_report = toReportEntry(ru_less_lous,"Local Unit", "LINKS","lurn","rurn","Reporting Unit","LINKS",appconf.PERIOD,"CHILDLESS", "Local Unit: No REU children found in LINKS table")
    //lous without parent Enterprise on table level
    val ent_less_lous = lous.join(ents,Seq("ern"), "left_anti")

    //lous without parent Enterprise on links level
    val ent_less_lous_links = louLinks.join(entLinks.drop("leus").drop("rus").withColumn("lurn", explode_outer(col("lous"))),Seq("ern"), "left_anti")

    //lous links refer to non-existing rous (table) entities
    val lous_links_with_fantom_rous_parents = louLinks.join(ents, Seq("ern"), "left_anti")

    //lous links refer to non-existing rous (table) entities
    val lous_links_with_fantom_ents_parents = louLinks.join(rus, Seq("rurn"), "left_anti")



    /**
      * Reporting Units
      * */
    //rus without parent Enterprise on table level
    val ent_less_rus = rus.join(ents, Seq("ern"),"left_anti")
    //rus without parent Enterprise on links level
    val ent_less_rus_links = ruLinks.join(entLinks.drop("leus").drop("lous").withColumn("rurn", explode_outer(col("rus"))),Seq("ern"), "left_anti")

    //rus linked to non-existing LOUs children
    val rus_fantom_lous_children_links = ruLinks.withColumn("lurn", explode_outer(col("lous"))).join(louLinks, Seq("ern"),"left_anti")

    //REUs records in Links referring to non-existing enterprise units in ENT table
    val rus_links_with_fantom_ent_parents = ruLinks.drop("lous").join(ents,Seq("ern"), "left_anti")


    //REUs records in Links referring to non-existing enterprise units in ENT table
    val rus_links_with_fantom_lu_children = ruLinks.withColumn("lurn", explode_outer(col("lous"))).join(lous,Seq("lurn"),"left_anti")




  /**
    * Legal Units
    * */
    val not_linked_leus = leus.join(entLinks, Seq("ern"),"left_anti")
    val no_table_ref_leus_links = entLinks.join(leus, Seq("ern"),"left_anti")
    val ent_less_leus_links = leuLinks.join(entLinks.drop("rus").drop("lous").withColumn("ubrn", explode_outer(col("leus"))),Seq("ern"), "left_anti")

    val ent_less_leus = leus.join(ents, Seq("ern"),"left_anti")




  /**
    * Enterprise Units
    * */

    //ents not registered in links
   val not_linked_ents = ents.join(entLinks,Seq("ern"),"left_anti")
   val no_table_ref_ent_links = entLinks.join(ents,Seq("ern"),"left_anti")
    //on ENT table level
   val ents_with_fantom_leu_children = ents.join(leus, Seq("ern"),"left_anti")
   val ents_with_fantom_ru_children = ents.join(rus, Seq("ern"),"left_anti")
   val ents_with_fantom_lou_children = ents.join(lous, Seq("ern"),"left_anti")



  /**
    * Admin data: VAT, PAYE, CH
    * */
    //links level
    val vatFlattennedLeus = leuLinks.select("ubrn","vats").withColumn("vat", explode_outer(col("vats"))).cache()
    val leu_less_vat_links = vatLinks.join(vatFlattennedLeus,Seq("ubrn"),"left_anti")
    val leu_with_fantom_vats = vatFlattennedLeus.join(vatLinks,Seq("ubrn"),"left_anti")
    vatFlattennedLeus.unpersist()

    val payeFlattennedLeusLinks = leuLinks.select("ubrn","payes").withColumn("paye", explode_outer(col("payes"))).cache()
    val leu_less_paye_links = payeLinks.join(vatFlattennedLeus,Seq("ubrn"),"left_anti")
    val leu_with_fantom_paye = payeFlattennedLeusLinks.join(vatLinks,Seq("ubrn"),"left_anti")
    payeFlattennedLeusLinks.unpersist()

    val leu_less_link_ch = chLinks.join(leuLinks.select("crn","ubrn"),Seq("crn","ubrn"),"left_anti")
    val ch_link_with_phantom_leu = leuLinks.select("crn","ubrn").join(chLinks,Seq("crn","ubrn"),"left_anti")


    //table to links level
    val leu_less_ch = chLinks.join(leus,Seq("crn","ubrn"),"left_anti")
    val leu_with_fantom_ch_links = leus.join(chLinks,Seq("crn","ubrn"),"left_anti")





    ents.unpersist()
    leus.unpersist()
    lous.unpersist()
    rus.unpersist()
    entLinks.unpersist()
    leuLinks.unpersist()

  }
}

