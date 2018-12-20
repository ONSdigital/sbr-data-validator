package spark.extensions

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

package object df {

  val reportRowSchema = new StructType()
                            .add(StructField("primary unit type", StringType,false))
                            .add(StructField("p.u. table", StringType,true))
                            .add(StructField("p.u. id name", StringType,true))
                            .add(StructField("p.u. id value", StringType,true))
                            .add(StructField("fk to s.u. name", StringType,true))
                            .add(StructField("fk to s.u. value", StringType,true))
                            .add(StructField("secondary unit type", StringType,true))
                            .add(StructField("s.u. table", StringType,true))
                            .add(StructField("period", StringType,true))
                            .add(StructField("problem type", StringType,true))
                            .add(StructField("message", StringType,true))



  val louRowSchema = new StructType()
                              .add(StructField("lurn", StringType,false))
                              .add(StructField("ern", StringType,false))
                              .add(StructField("rurn", StringType,false))


  val linksLouRowSchema = new StructType()
                              .add(StructField("lurn", StringType,false))
                              .add(StructField("rurn", StringType,false))
                              .add(StructField("ern", StringType,false))


  val leuRowSchema = new StructType()
                              .add(StructField("ubrn", StringType,false))
                              .add(StructField("ern", StringType,false))
                              .add(StructField("crn", StringType,true))


  val linksLeuRowSchema = new StructType()
                              .add(StructField("ubrn", StringType,false))
                              .add(StructField("ern", StringType,true))
                              .add(StructField("crn", StringType,true))
                              .add(StructField("payes", ArrayType(StringType,true),true))
                              .add(StructField("vats", ArrayType(StringType,true),true))


  val linksChRowSchema =  new StructType()
                              .add(StructField("crn", StringType,false))
                              .add(StructField("ubrn", StringType,true))


  val linksPayeRowSchema =  new StructType()
                              .add(StructField("paye", StringType,false))
                              .add(StructField("ubrn", StringType,true))



  val linksVatRowSchema =  new StructType()
                              .add(StructField("vat", StringType,false))
                              .add(StructField("ubrn", StringType,true))




  val ruRowSchema = new StructType()
                              .add(StructField("rurn", StringType,false))
                              .add(StructField("ern", StringType,false))


  val linksRuRowSchema = new StructType()
                              .add(StructField("rurn", StringType,false))
    .add(StructField("ern", StringType,true))
    .add(StructField("lous", ArrayType(StringType,true),true))


  val entRowSchema = new StructType()
                              .add(StructField("ern", StringType,false))


  val linksEntRowSchema = new StructType()
                              .add(StructField("ern", StringType,false))
                              .add(StructField("leus", ArrayType(StringType,true),true))
                              .add(StructField("lous", ArrayType(StringType,true),true))
                              .add(StructField("rus", ArrayType(StringType,true),true))


  val existingLuBiRowSchema = new StructType()
    .add(StructField("id", StringType,true))
    .add(StructField("ern", StringType,true))


  implicit class DataFrameExtensions(df:DataFrame){

    def castAllToString() =  df.schema.fields.foldLeft(df)((dataFrame, field) => field.dataType match{
      case ArrayType(LongType, nullability) =>  dataFrame.withColumn(field.name,df.col(field.name).cast((ArrayType(StringType,nullability))))
      case ArrayType(IntegerType, nullability) =>  dataFrame.withColumn(field.name,df.col(field.name).cast((ArrayType(StringType,nullability))))
      case ArrayType(StringType, nullability) =>  dataFrame
      case _  => dataFrame.withColumn(field.name,df.col(field.name).cast((StringType)))
    }
    )

  }

  implicit class SqlRowExtensions(val row:Row) {

    def getStringWithNull(field:String): Option[String] = {
      val v = getValue[String](field)
      if (v.isDefined && v==null) None
      else v
    }
    /**
      * returns option of value
      * Retuns None if field is present but value is null or if field is not present
      * returns Some(VALUE_OF_THE_FIELD) otherwise
      * */
    def getOption[T](field:String)= {
      if(row.isNull(field)) None
      else Option[T](row.getAs[T](field))
    }

    /**
      * Returns None if:
      * 1. row does not contain field with given name
      * 2. value is null
      * 3. value's data type is not String
      * returns Some(...) of value
      * otherwise
      * */
    def getStringOption(name:String) = {
      getOption[String](name)
    }

    def getValueOrEmptyStr(fieldName:String) = getStringValueOrDefault(fieldName,"")

    def getStringValueOrDefault(fieldName:String,default:String) = getStringOption(fieldName).getOrElse(default)

    def getValueOrNull(fieldName:String) = getStringOption(fieldName).getOrElse(null)

    def getString(field:String): Option[String] = getValue[String](field)

    def getLong(field:String): Option[Long] = getValue[Long](field)

    def getInteger(field:String): Option[Int] = getValue[Int](field)

    def getStringSeq(field:String): Option[Seq[String]] = getSeq(field,Some((s:String) => s.trim.nonEmpty))

    def getLongSeq(field:String): Option[Seq[Long]] = getSeq[Long](field)

    def getSeq[T](fieldName:String, eval:Option[T => Boolean] = None): Option[Seq[T]] = if(isNull(fieldName)) None else Some(row.getSeq[T](row.fieldIndex(fieldName)).filter(v => v!=null && eval.map(_(v)).getOrElse(true)))

    def isNull(field:String) = try {
      row.isNullAt(row.fieldIndex(field))  //if field exists and the value is null
    }catch {
      case iae:IllegalArgumentException => true  //if field does not exist
      case e: Throwable => {
        println(s"field: ${if (field==null) "null" else field.toString()}")
        throw e
      }
    }

    def getCalcValue(fieldName:String): Option[String] = {
      val v = isNull(fieldName)
      v match{
        case true  => Some("")
        case false => Some(row.getAs(fieldName).toString)
      }}

    def getValue[T](
                     fieldName:String,
                     eval:Option[T => Boolean] = None
                   ): Option[T] = if(isNull(fieldName)) None else {

      val v = row.getAs[T](fieldName)
      if (v.isInstanceOf[String] && v.asInstanceOf[String].trim.isEmpty) None
      else if (v==null) None
      else eval match{
        case Some(f) => if(f(v)) Some(v) else None
        case None  => Some(v)
      }}
  }



}
