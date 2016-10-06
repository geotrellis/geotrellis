/*
 * Copyright (c) 2014 DigitalGlobe.
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
 */

package geotrellis

import geotrellis.spark.TestEnvironment

import org.apache.spark.SparkConf
import org.scalatest._

trait GeoMesaTestEnvironment extends TestEnvironment { self: Suite =>
  override def setKryoRegistrator(conf: SparkConf) =
    conf.set("spark.kryo.registrator", classOf[geotrellis.spark.io.geomesa.kryo.KryoRegistrator].getName)
        .set("spark.kryo.registrationRequired", "false")
}
