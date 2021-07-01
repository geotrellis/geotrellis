/*
 * Copyright 2016 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.store

import geotrellis.store.util.BlockingThreadPool

import cats.effect.IO
import cats.syntax.apply._
import cats.syntax.either._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

abstract class AsyncWriter[Client, V, E](executionContext: => ExecutionContext = BlockingThreadPool.executionContext) extends Serializable {

  def readRecord(client: Client, key: String): Try[V]

  def encodeRecord(key: String, value: V): E

  def writeRecord(client: Client, key: String, encoded: E): Try[Long]

  def write(
    client: Client,
    partition: Iterator[(String, V)],
    mergeFunc: Option[(V, V) => V] = None,
    retryFunc: Option[Throwable => Boolean]
  ): Unit = {
    if (partition.isEmpty) return
    val rows: fs2.Stream[IO, (String, V)] = fs2.Stream.fromIterator[IO](partition, 1)

    def elaborateRow(row: (String, V)): fs2.Stream[IO, (String, V)] = {
      val foldUpdate: ((String, V)) => (String, V) = { case newRecord @ (key, newValue) =>
        mergeFunc match {
          case Some(fn) =>
            // TODO: match on this failure to retry reads
            readRecord(client, key) match {
              case Success(oldValue) => (key, fn(newValue, oldValue))
              case Failure(_) => newRecord
            }
          case None => newRecord
        }
      }

      fs2.Stream eval /*IO.shift(ec) *>*/ IO(foldUpdate(row))
    }

    def encode(row: (String, V)): fs2.Stream[IO, (String, E)] = {
      val (key, value) = row
      val encodeTask = IO((key, encodeRecord(key, value)))
      fs2.Stream eval /*IO.shift(ec) *>*/ encodeTask
    }

    def retire(row: (String, E)): fs2.Stream[IO, Try[Long]] = {
      val writeTask = IO(writeRecord(client, row._1, row._2))
      import geotrellis.store.util.IOUtils._
      fs2.Stream eval /*IO.shift(ec) *>*/ retryFunc.fold(writeTask)(writeTask.retryEBO(_))
    }

    import cats.effect.unsafe.implicits.global
    rows
      .flatMap(elaborateRow)
      .flatMap(encode)
      .map(retire)
      .parJoinUnbounded
      .compile
      .toVector
      .attempt
      .unsafeRunSync()
      .valueOr(throw _)
  }
}
