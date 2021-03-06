package dao.hbase

import global.{AppParams, Configs}
import model.HFileRow
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{PrefixFilter, RegexStringComparator, RowFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession}
import org.slf4j.LoggerFactory

/**
  *
  */
trait HBaseDao extends Serializable{
  import global.Configs._

  val logger = LoggerFactory.getLogger(getClass)

  def readTable(appParams:AppParams, config:Configuration, tableName:String)(implicit spark:SparkSession) = {
    config.set(TableInputFormat.INPUT_TABLE, tableName)
    val res = readKvsFromHBase(config)
    config.unset(TableInputFormat.INPUT_TABLE)
    res
  }


  def truncateTables(implicit connection:Connection,appParams:AppParams) = {
    truncateLinksTable
    truncateEntsTable
    truncateLousTable
    truncateLeusTable
    truncateRusTable
  }

  def truncateTable(tableName:String)(implicit connection:Connection,appParams:AppParams) =  wrapTransaction(tableName){ (table, admin) =>
    admin.disableTable(table.getName)
    admin.truncateTable(table.getName,true)
  }

  def truncateLinksTable(implicit connection:Connection,appParams:AppParams) =  truncateTable(linksTableName(appParams))
  def truncateEntsTable(implicit connection:Connection,appParams:AppParams) =  truncateTable(entsTableName(appParams))
  def truncateLousTable(implicit connection:Connection,appParams:AppParams) =  truncateTable(lousTableName(appParams))
  def truncateLeusTable(implicit connection:Connection,appParams:AppParams) =  truncateTable(leusTableName(appParams))
  def truncateRusTable(implicit connection:Connection,appParams:AppParams) =  truncateTable(rusTableName(appParams))

  def readDeleteData(appParams:AppParams,regex:String)(implicit spark:SparkSession): Unit = {
    val localConfCopy = conf
    val data: RDD[HFileRow] = readLinksWithKeyFilter(localConfCopy,appParams,regex)
    val rows: Array[HFileRow] = data.take(5)
    rows.map(_.toString).foreach(row => print(
      "="*10+
        row+'\n'+
        "="*10
    ))
  }

  def readLinksWithKeyFilter(confs:Configuration, appParams:AppParams, regex:String)(implicit spark:SparkSession): RDD[HFileRow] = {
    readTableWithKeyFilter(confs, appParams, linksTableName(appParams), regex)
  }

  def readLinksWithKeyPrefixFilter(confs:Configuration, appParams:AppParams, prefix:String)(implicit spark:SparkSession): RDD[HFileRow] = {
    readTableWithPrefixKeyFilter(confs, appParams, linksTableName(appParams), prefix)
  }

  def readLouWithKeyFilter(confs:Configuration,appParams:AppParams, regex:String)(implicit spark:SparkSession): RDD[HFileRow] = {
    readTableWithKeyFilter(confs, appParams, lousTableName(appParams), regex)
  }

  def readEnterprisesWithKeyFilter(confs:Configuration,appParams:AppParams, regex:String)(implicit spark:SparkSession): RDD[HFileRow] = {

    readTableWithKeyFilter(confs, appParams, entsTableName(appParams), regex)
  }


  def readTableWithPrefixKeyFilter(confs:Configuration,appParams:AppParams, tableName:String, regex:String)(implicit spark:SparkSession) = {
    val localConfCopy = confs
    withKeyPrefixScanner(localConfCopy,regex,appParams,tableName){
      readKvsFromHBase
    }}


  def readTableWithKeyFilter(confs:Configuration,appParams:AppParams, tableName:String, regex:String)(implicit spark:SparkSession) = {
    val localConfCopy = confs
    withScanner(localConfCopy,regex,appParams,tableName){
      readKvsFromHBase
    }}

  def loadLinksHFile(implicit connection:Connection,appParams:AppParams) = wrapTransaction(linksTableName(appParams)){ (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(appParams.PATH_TO_LINKS_HFILE), admin,table,regionLocator)
  }

  def loadEnterprisesHFile(implicit connection:Connection,appParams:AppParams) = wrapTransaction(entsTableName(appParams)){ (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(appParams.PATH_TO_ENTERPRISE_HFILE), admin,table,regionLocator)
  }


  def loadLousHFile(implicit connection:Connection,appParams:AppParams) = wrapTransaction(lousTableName(appParams)){ (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(appParams.PATH_TO_LOCALUNITS_HFILE), admin,table,regionLocator)
  }


  def loadLeusHFile(implicit connection:Connection,appParams:AppParams) = wrapTransaction(leusTableName(appParams)){ (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(appParams.PATH_TO_LEGALUNITS_HFILE), admin,table,regionLocator)
  }

  def loadRusHFile(implicit connection:Connection,appParams:AppParams) = wrapTransaction(rusTableName(appParams)){ (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(appParams.PATH_TO_REPORTINGUNITS_HFILE), admin,table,regionLocator)
  }


  private def wrapTransaction(fullTableName:String)(action:(Table,Admin) => Unit)(implicit connection:Connection){
    val tn = TableName.valueOf(fullTableName)
    val table: Table = connection.getTable(tn)
    val admin = connection.getAdmin
    setJob(table)
    action(table,admin)
    table.close
  }


  private def wrapReadTransaction(tableName:String)(action: String => RDD[HFileRow])(implicit connection:Connection):RDD[HFileRow] = {
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    val admin = connection.getAdmin
    setJob(table)
    val res = action(tableName)
    table.close
    res
  }


  private def setJob(table:Table)(implicit connection:Connection){
    val job = Job.getInstance(connection.getConfiguration)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, table)
    //HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(table.getName))
  }

  def withKeyPrefixScanner(config:Configuration,prefix:String, appParams:AppParams, tableName:String)(getResult:(Configuration) => RDD[HFileRow]): RDD[HFileRow] = {
    config.set(TableInputFormat.INPUT_TABLE, tableName)
    setPrefixScanner(config,prefix,appParams)
    val res = getResult(config)
    unsetPrefixScanner(config)
    config.unset(TableInputFormat.INPUT_TABLE)
    res
  }

  def withScanner(config:Configuration,regex:String, appParams:AppParams, tableName:String)(getResult:(Configuration) => RDD[HFileRow]): RDD[HFileRow] = {
    config.set(TableInputFormat.INPUT_TABLE, tableName)
    setScanner(config,regex,appParams)
    val res = getResult(config)
    unsetScanner(config)
    config.unset(TableInputFormat.INPUT_TABLE)
    res
  }

  def readKvsFromHBase(configuration:Configuration)(implicit spark:SparkSession): RDD[HFileRow] =  {
    spark.sparkContext.newAPIHadoopRDD(
      configuration,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
      .map(row => HFileRow(row._2))
  }

  def copyExistingRecordsToHFiles(appParams:AppParams,dirName:String = "existing")(implicit spark:SparkSession) = {
    def buildPath(path:String) = {
      val dirs = path.split("/")
      val updatedDirs = (dirs.init :+ dirName) :+ dirs.last
      val res = updatedDirs.mkString("/")
      res
    }



  }

  private def unsetScanner(config:Configuration) = config.unset(TableInputFormat.SCAN)

  private def setScanner(config:Configuration,regex:String, appParams:AppParams) = {

    val comparator = new RegexStringComparator(regex)
    val filter = new RowFilter(CompareOp.EQUAL, comparator)

    def convertScanToString(scan: Scan): String = {
      val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
      return Base64.encodeBytes(proto.toByteArray())
    }

    val scan = new Scan()
    scan.setFilter(filter)
    val scanStr = convertScanToString(scan)

    config.set(TableInputFormat.SCAN,scanStr)
  }


  private def unsetPrefixScanner(config:Configuration) = config.unset(TableInputFormat.SCAN)

  private def setPrefixScanner(config:Configuration,prefix:String, appParams:AppParams) = {

    val prefixFilter = new PrefixFilter(prefix.getBytes)
    val scan: Scan = new Scan()
    scan.setFilter(prefixFilter)

    def convertScanToString: String = {
      val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
      return Base64.encodeBytes(proto.toByteArray())
    }


    config.set(TableInputFormat.SCAN,convertScanToString)
  }



  def linksTableName(appconf:AppParams) =  s"${appconf.HBASE_LINKS_TABLE_NAMESPACE}:${appconf.HBASE_LINKS_TABLE_NAME}"
  def leusTableName(appconf:AppParams) =  s"${appconf.HBASE_LEGALUNITS_TABLE_NAMESPACE}:${appconf.HBASE_LEGALUNITS_TABLE_NAME}"
  def lousTableName(appconf:AppParams) =  s"${appconf.HBASE_LOCALUNITS_TABLE_NAMESPACE}:${appconf.HBASE_LOCALUNITS_TABLE_NAME}"
  def rusTableName(appconf:AppParams) =  s"${appconf.HBASE_REPORTINGUNITS_TABLE_NAMESPACE}:${appconf.HBASE_REPORTINGUNITS_TABLE_NAME}"
  def entsTableName(appconf:AppParams) =  s"${appconf.HBASE_ENTERPRISE_TABLE_NAMESPACE}:${appconf.HBASE_ENTERPRISE_TABLE_NAME}"

}

object HBaseDao extends HBaseDao