package global

case class AppParams(
                      HBASE_LINKS_TABLE_NAME:String,
                      HBASE_LINKS_TABLE_NAMESPACE:String,
                      HBASE_LINKS_COLUMN_FAMILY:String,

                      HBASE_LEGALUNITS_TABLE_NAME:String,
                      HBASE_LEGALUNITS_TABLE_NAMESPACE:String,
                      HBASE_LEGALUNITS_COLUMN_FAMILY:String,

                      HBASE_ENTERPRISE_TABLE_NAME:String,
                      HBASE_ENTERPRISE_TABLE_NAMESPACE:String,
                      HBASE_ENTERPRISE_COLUMN_FAMILY:String,

                      HBASE_LOCALUNITS_TABLE_NAME:String,
                      HBASE_LOCALUNITS_TABLE_NAMESPACE:String,
                      HBASE_LOCALUNITS_COLUMN_FAMILY:String,

                      HBASE_REPORTINGUNITS_TABLE_NAME:String,
                      HBASE_REPORTINGUNITS_TABLE_NAMESPACE:String,
                      HBASE_REPORTINGUNITS_COLUMN_FAMILY:String,

                      PATH_TO_LEGAL_UNIT_INTEGRITY_REPORT:String,
                      PATH_TO_LOGAL_UNIT_INTEGRITY_REPORT:String,
                      PATH_TO_REPORTING_UNIT_INTEGRITY_REPORT:String,
                      PATH_TO_ENTERPRISE_UNIT_INTEGRITY_REPORT:String,
                      ENV:String
                    )

object AppParams{
  def apply(args:Array[String]) = new AppParams(
                                                            args(0),
                                                            args(1),
                                                            args(2),
                                                            args(3),
                                                            args(4),
                                                            args(5),
                                                            args(6),
                                                            args(7),
                                                            args(8),
                                                            args(9),
                                                            args(10),
                                                            args(11),
                                                            args(12),
                                                            args(13),
                                                            args(14),
                                                            args(15),
                                                            args(16),
                                                            args(17),
                                                            args(18),
                                                            args(19)
                                                          )

}
