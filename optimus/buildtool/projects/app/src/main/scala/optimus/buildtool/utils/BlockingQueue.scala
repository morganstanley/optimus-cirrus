/*
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package optimus.buildtool.utils

import java.util.concurrent.LinkedBlockingQueue

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq

class BlockingQueue[A] {
  val inner = new LinkedBlockingQueue[A]

  def take(): A = inner.take()

  def put(a: A): Unit = inner.put(a)

  def poll(): Option[A] = Option(inner.poll())

  def pollAll(): Seq[A] = {
    val l = new java.util.ArrayList[A]
    inner.drainTo(l)
    l.asScala.toIndexedSeq
  }

  def size: Int = inner.size()

  def clear(): Unit = inner.clear()
}

object BlockingQueue {

  def populate[A](es: A*): BlockingQueue[A] = {
    val q = new BlockingQueue[A]
    es.foreach(q.put)
    q
  }
}
