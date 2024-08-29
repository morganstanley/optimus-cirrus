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
package optimus.dsi.base

import java.util.Collections

import scala.jdk.CollectionConverters._
import msjava.slf4jutils.scalalog._

/**
 * Scala ObservableMap is deprecated. This one is adapted from
 * http://stackoverflow.com/questions/13435151/implementing-observer-pattern
 */
// TODO (OPTIMUS-57177): license might be needed in order to publish this code as open-source
trait Observer[S] {
  def receiveUpdate(subject: S): Unit
}

object Subject {
  val log = getLogger[Subject.type]
}

trait Subject[S] { this: S =>
  import Subject.log

  // synchronizedMap gives us a concurrency safe map implementing the unsafe IdentityMap
  private val observers = Collections.synchronizedMap(new java.util.IdentityHashMap[Observer[S], Boolean])

  def addObserver(observer: Observer[S]): Boolean = observers.put(observer, true)
  def removeObserver(observer: Observer[S]): Boolean = observers.remove(observer)
  def notifyObservers(): Unit = {
    log.debug("notifyObservers called on {}", observers.keySet())
    observers.keySet().asScala.foreach(_.receiveUpdate(this))
  }
}
