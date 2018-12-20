package model

import global.Configs
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HConstants, KeyValue}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import spark.rdd.HBaseDataReader

import scala.util.Try

/**
  *
  */


case class HFileRow(key:String, cells:Iterable[KVCell[String,String]]) {

  def getLinkId = key.split("~").last

  def getValueOrNull(key: String, byKey: Boolean = true) = getCellValue(key,byKey).getOrElse(null)

  def getValueOrStr(key: String, byKey: Boolean = true,default:String = "") = getCellValue(key,byKey).getOrElse(default)

  def getCellValue(key: String, byKey: Boolean) = if (byKey) cells.collect { case KVCell(`key`, value) => Option(value)}.headOption.flatten//.getOrElse(null)
  else cells.collect { case KVCell(value, `key`) => Option(value)}.headOption.flatten//.getOrElse(null)

  def getCellArrayValue(key: String) = {

    val result = cells.collect { case KVCell(value, `key`) => value }
    if (result.isEmpty) null
    else result
  }

  override def equals(obj: scala.Any): Boolean = obj match {
    case HFileRow(otherKey, otherCells) if (
      (otherKey == this.key) && (this.cells.toSet == otherCells.toSet)
      ) => true
    case _ => false
  }

  //put type set as default
  def toHFileCells(columnFamily: String, kvType: Int = KeyValue.Type.Put.ordinal()) = cells.map(cell => HFileCell(key, columnFamily, cell.column, cell.value, kvType))

  def toEntRow = {
    import spark.extensions.df._
    try {
      new GenericRowWithSchema(Array( getValueOrStr("ern"),
        getValueOrStr("prn",default=Configs.DEFAULT_PRN),
        getValueOrNull("entref")
      ), entRowSchema)
    } catch {
      case e: java.lang.RuntimeException => {
        println(s"(toEntRow)Exception reading enterprise row with ern: ${getValueOrStr("ern")}")
        throw e
      }
    }
  }


  def toLeuRow = {
    import spark.extensions.df._

    try {
      new GenericRowWithSchema(Array(

        getValueOrStr("ubrn"),
        key.split("~").head.reverse,//ern
        //getValueOrStr("ern"),
        getValueOrNull("crn")
      ), leuRowSchema)
    } catch {
      case e: java.lang.RuntimeException => {
        println(s"(toLuRow)Exception reading enterprise row with ern: ${getValueOrStr("ern")}")
        throw e
      }
    }}

  def toRuRow = {
    import spark.extensions.df._

    new GenericRowWithSchema(Array(
      getValueOrStr("rurn"),
      getValueOrStr("ern")
    ),ruRowSchema)
  }

  def toEntLinkRow = {
    import spark.extensions.df._
    new GenericRowWithSchema(Array(
      getLinkId,
      Try {
        getCellArrayValue("LEU").map(paye => if (paye.startsWith("c_")) {
          paye.substring(2)
        } else paye)
      }.getOrElse(null),
      Try {
        getCellArrayValue("LOU").map(paye => if (paye.startsWith("c_")) {
          paye.substring(2)
        } else paye)
      }.getOrElse(null),
      Try {
        getCellArrayValue("REU").map(vat => if (vat.startsWith("c_")) {
          vat.substring(2)
        } else vat)
      }.getOrElse(null)
    ),linksEntRowSchema)

  }


  def toLeuLinksRow = {
    import spark.extensions.df._

    new GenericRowWithSchema(Array(
                                    getLinkId,
                                    getValueOrStr("p_ENT"),
                                    getCellValue("CH", false).map(_.substring(2)).getOrElse(null),
                                    Try {
                                      getCellArrayValue("PAYE").map(paye => if (paye.startsWith("c_")) {
                                        paye.substring(2)
                                      } else paye)
                                    }.getOrElse(null),
                                    Try {
                                      getCellArrayValue("VAT").map(vat => if (vat.startsWith("c_")) {
                                        vat.substring(2)
                                      } else vat)
                                    }.getOrElse(null)
                                    )
                          , linksLeuRowSchema)



  }

  def toLouLinksRow = {
    import spark.extensions.df._

    new GenericRowWithSchema(Array(
      getLinkId,
      getValueOrStr("p_REU"),
      getValueOrStr("p_ENT")
    ), linksLouRowSchema)
  }

  def toVatLinksRow = {
    import spark.extensions.df._

    new GenericRowWithSchema(Array(
      getLinkId,
      getValueOrStr("p_LEU")
    ), linksVatRowSchema)
  }

  def toPayeLinksRow = {
    import spark.extensions.df._

    new GenericRowWithSchema(Array(
      getLinkId,
      getValueOrStr("p_LEU")
    ), linksPayeRowSchema)
  }


  def toChLinksRow = {
    import spark.extensions.df._

    new GenericRowWithSchema(Array(
      getLinkId,
      getValueOrStr("p_LEU")
    ), linksChRowSchema)
  }



  def toRuLinksRow = {
    import spark.extensions.df._

    new GenericRowWithSchema(Array(
      getLinkId,
      getValueOrStr("p_ENT"),
      Try {
        getCellArrayValue("LOU").map(paye => if (paye.startsWith("c_")) {
          paye.substring(2)
        } else paye)
      }.getOrElse(null)
    ), linksRuRowSchema)
  }



  def toUbrnErnRow = {
    import spark.extensions.df._

    new GenericRowWithSchema(Array(
      getLinkId,
      getValueOrStr("p_ENT")
    ), existingLuBiRowSchema)
  }

  def toLouRow = {
    import spark.extensions.df._

    try {
      new GenericRowWithSchema(Array(

        getValueOrStr("lurn"),
        getValueOrStr("ern"),
        getValueOrStr("rurn")
      ), louRowSchema)
    }catch {
      case e: java.lang.RuntimeException => {
        println(s"Exception reading enterprise row with ern: ${getValueOrStr("ern")}")
        throw e
      }
    }
  }

  def toHFileCellRow(colFamily: String): Iterable[(String, HFileCell)] = {
    cells.map(cell => (key, HFileCell(key, colFamily, cell.column, cell.value)))
  }

  def toPutHFileEntries(colFamily: String): Iterable[(ImmutableBytesWritable, KeyValue)] = {
    cells.flatMap(kv =>
      Seq((new ImmutableBytesWritable(key.getBytes()), new KeyValue(key.getBytes, colFamily.getBytes, kv.column.getBytes, kv.value.getBytes)))
    )
  }

  def toDeleteHFileEntries(colFamily: String): Iterable[(ImmutableBytesWritable, KeyValue)] = {
    val excludedColumns = Seq("p_ENT")
    if (key.contains("~LEU~")) {
      cells.filterNot(cell => excludedColumns.contains(cell.column)).flatMap(kv =>
        Seq((new ImmutableBytesWritable(key.getBytes()), new KeyValue(key.getBytes, colFamily.getBytes, kv.column.getBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn)))
      )
    } else {
      val cell = cells.head
      Seq((new ImmutableBytesWritable(key.getBytes()), new KeyValue(key.getBytes, colFamily.getBytes, cell.column.getBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteFamily)))
    }
  }

  def toDeletePeriodHFileEntries(colFamily: String): Iterable[(ImmutableBytesWritable, KeyValue)] =
    Seq((new ImmutableBytesWritable(key.getBytes()), new KeyValue(key.getBytes, colFamily.getBytes, cells.head.column.getBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteFamily)))


  def toDeleteHFile(colFamily: String): Iterable[(ImmutableBytesWritable, KeyValue)] = {
    val excludedColumns = Seq("p_ENT")
    if (key.contains("~LEU~")) {
      cells.filterNot(cell => excludedColumns.contains(cell.column)).flatMap(kv =>
        Seq((new ImmutableBytesWritable(key.getBytes()), new KeyValue(key.getBytes, colFamily.getBytes, kv.column.getBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn)))
      )
    } else {
      val cell = cells.head
      Seq((new ImmutableBytesWritable(key.getBytes()), new KeyValue(key.getBytes, colFamily.getBytes, cell.column.getBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteFamily)))
    }
  }


  def toDeleteHFileRows(colFamily: String): Iterable[(String, HFileCell)] = {
    val excludedColumns = Seq("p_ENT")
    if (key.contains("~LEU~")) {
      cells.filterNot(cell => excludedColumns.contains(cell.column)).flatMap(kv =>
        Seq((key, new HFileCell(key, colFamily, kv.column, "", HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn.ordinal())))
      )
    } else {
      val cell = cells.head //delete is made on row level, so there's no need to repeat delete for every column
      Seq((key, new HFileCell(key, colFamily, cell.column, "", HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn.ordinal())))
    }
  }


  def toDeleteColumnsExcept(colFamily: String, columns: Seq[String]): Iterable[KeyValue] = cells.filterNot(cell => columns.contains(cell.column)).map(kv =>

    new KeyValue(key.getBytes, colFamily.getBytes, kv.column.getBytes, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn)
  )

  def toPrintString = {
    val key = this.key
    val cellsToString = cells.map(cell => " \t" + cell.toPrintString).mkString("\n")

    '\n' +
      "key: " + key +
      '\n' +
      " cells: " +
      cellsToString
  }
}
object HFileRow {

  def apply(entry: (String, Iterable[(String, String)])) = new HFileRow(entry._1, entry._2.map(c => KVCell(c)).toSeq)

  def apply(result: Result) = {
    val rowKey = Bytes.toString(result.getRow)
    val cells: Array[(String, String)] = result.rawCells().map(c => HBaseDataReader.getKeyValue(c)._2)
    new HFileRow(rowKey, cells.map(cell => KVCell(cell._1.trim(), cell._2.trim())))
  }

  implicit def buildFromHFileDataMap(entry: (String, Iterable[(String, String)])) = HFileRow(entry)
}