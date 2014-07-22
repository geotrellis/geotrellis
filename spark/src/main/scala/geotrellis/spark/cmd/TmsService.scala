package geotrellis.spark.cmd

import geotrellis.spark.cmd.args.{SparkArgs, HadoopArgs}
import geotrellis.spark.service.TmsHttpActor

import org.apache.hadoop.fs.Path
import org.apache.spark.Logging

import akka.actor.{ActorSystem, Props}
import akka.io.IO

import spray.can.Http

import com.quantifind.sumac.ArgMain
import com.quantifind.sumac.validation.Required

class TmsArgs extends SparkArgs with HadoopArgs {
  @Required var root: String = _
}

object TMS extends ArgMain[TmsArgs] {
  def main(args: TmsArgs) {
    implicit val system = ActorSystem()
    val service = system.actorOf(TmsHttpActor.props(args), "tms-service")
    //This is how NOT to do it, what happend to config? //TODO - make config
    IO(Http) ! Http.Bind(service, interface = "0.0.0.0", port = 8000)
  }
}