package spark.extensions

import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

package object df {

  val reportRowSchema = new StructType()
                            .add(StructField("primary unit", StringType,false))
                            .add(StructField("p.u. table", StringType,true))
                            .add(StructField("p.u. id", StringType,true))
                            .add(StructField("secondary unit", StringType,true))
                            .add(StructField("s.u. table", StringType,true))
                            .add(StructField("description", StringType,true))



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



}
