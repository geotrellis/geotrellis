/*******************************************************************************
 * Copyright (c) 2014 Azavea.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package geotrellis.jetty

import com.typesafe.config.{ConfigFactory,Config}

import scala.collection.mutable

case class ServerConfig(packages:Seq[String],
                        jetty:JettyConfig,
                        staticContentPath:Option[String])

object ServerConfig {
  def init():ServerConfig = init(ConfigFactory.load())
  def init(config:Config):ServerConfig = {
    val packages = mutable.Set[String]()
  
    if(config.hasPath("geotrellis.rest-package")) {
      packages += config.getString("geotrellis.rest-package")
    }

    val staticContentPath = if(config.hasPath("geotrellis.server.serve-static")) {
      if(config.getBoolean("geotrellis.server.serve-static")) {
        Some(config.getString("geotrellis.server.static-path"))
      } else { None }
    } else { None }

    val jetty = JettyConfig.init(config)

    val includeServices = config.hasPath("geotrellis.jetty.include-gt-services") &&
                          config.getBoolean("geotrellis.jetty.include-gt-services")

    if(includeServices) {
      packages += "geotrellis.jetty.service"
    }

    ServerConfig(packages.toSeq,
                 jetty,
                 staticContentPath)
  }
}

case class JettyConfig(host:String,
                       port:Int,
                       corePoolSize:Int,
                       maximumPoolSize:Int,
                       keepAliveTime:Long,
                       serveStatic:Boolean,
                       contextPath:String)
object JettyConfig {
  def init():JettyConfig = init(ConfigFactory.load())
  def init(config:Config):JettyConfig = {
    val host = config.getString("geotrellis.host")
    val port = config.getInt("geotrellis.port")

    val corePoolSize = config.getInt("geotrellis.jetty.corePoolSize")
    val maximumPoolSize = config.getInt("geotrellis.jetty.maximumPoolSize")
    val keepAliveTime = config.getLong("geotrellis.jetty.keepAliveMilliseconds")

    val serveStatic = config.hasPath("geotrellis.server.serve-static") &&
                      config.getBoolean("geotrellis.server.serve-static")

    val contextPath = 
      if(config.hasPath("geotrellis.rest-prefix")) {
        val s = config.getString("geotrellis.rest-prefix") + "/*"
        if(!s.startsWith("/")) { s"/$s" }
        else { s }
      } else {
        "/gt/*"
      }

    JettyConfig(host,port,corePoolSize,maximumPoolSize,keepAliveTime,serveStatic,contextPath)
  }
}
