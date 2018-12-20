package validator

import global.AppParams
import global.Configs.conf
import service.DataValidationReportService

object Main extends DataValidationReportService{
  def main(args: Array[String]): Unit = {
    conf.set("hbase.zookeeper.quorum", args(0))
    conf.set("hbase.zookeeper.property.clientPort", args(1))
    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 500)
    val appParams = AppParams(args.takeRight(24))
    validate(appParams)
  }
}
