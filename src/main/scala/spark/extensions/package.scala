package spark.extensions

import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

package object df {


  val louRowSchema = new StructType()
                              .add(StructField("lurn", StringType,false))
                              .add(StructField("luref", StringType,true))
                              .add(StructField("ern", StringType,false))
                              .add(StructField("prn", StringType,false))
                              .add(StructField("rurn", StringType,false))
                              .add(StructField("ruref", StringType,true))
                              .add(StructField("entref", StringType,true))


  val linksLouRowSchema = new StructType()
                              .add(StructField("rurn", StringType,false))
                              .add(StructField("ern", StringType,false))


  val leuRowSchema = new StructType()
                              .add(StructField("ubrn", StringType,false))
                              .add(StructField("crn", StringType,true))
                              .add(StructField("uprn", StringType,false))


  val linksLeuRowSchema = new StructType()
                              .add(StructField("ubrn", StringType,false))
                              .add(StructField("ern", StringType,true))
                              .add(StructField("crn", StringType,true))
                              .add(StructField("paye", ArrayType(StringType,true),true))
                              .add(StructField("vat", ArrayType(StringType,true),true))


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
                              .add(StructField("entref", StringType,true))
                              .add(StructField("ruref", StringType,true))
                              .add(StructField("prn", StringType,false))


  val linksRuRowSchema = new StructType()
                              .add(StructField("rurn", StringType,false))
                              .add(StructField("lurn", StringType,true))
                              .add(StructField("ern", StringType,true))


  val entRowSchema = new StructType()
                              .add(StructField("ern", StringType,false))
                              .add(StructField("prn", StringType,false))
                              .add(StructField("entref", StringType,true))


  val linksEntRowSchema = new StructType()
                              .add(StructField("ern", StringType,false))
                              .add(StructField("leus", ArrayType(StringType,true),true))
                              .add(StructField("lous", ArrayType(StringType,true),true))


  val existingLuBiRowSchema = new StructType()
    .add(StructField("id", StringType,true))
    .add(StructField("ern", StringType,true))



}
