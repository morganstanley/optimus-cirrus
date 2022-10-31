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
package optimus.breadcrumbs

import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.Crumb.Headers
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.platform.util.Log

import scala.collection.mutable

abstract private[optimus] class BreadcrumbsAnalyzer extends BreadcrumbsPublisher {
  override def sendInternal(c: Crumb): Boolean = throw new NotImplementedError("Tests should override this")
  final override def collecting: Boolean = true
  override def init(): Unit = {}
  override def flush(): Unit = {}
  override def shutdown(): Unit = {}
}

class CrumbCounter extends BreadcrumbsAnalyzer with Log {
  import spray.json._
  import DefaultJsonProtocol._
  import optimus.breadcrumbs.crumbs.Properties._
  private val counts = mutable.Map.empty[String, Int]
  def count(id: ChainedID): Int = counts.getOrElse(id.toString, 0)
  private def addCount(uuid: String): Unit = {
    counts
      .get(uuid)
      .fold {
        counts.put(uuid, 1)
      } { i =>
        counts.put(uuid, i + 1)
      }
  }
  override def sendInternal(c: Crumb): Boolean = {
    val m = c.asJMap
    log.info(s"Sending: ${c.debugString}")
    val uuid = m.getAs[String](Headers.Uuid).get
    addCount(uuid)
    for {
      spySeq <- c.jsonProperties.get(Properties.currentlyRunning.toString)
      spy <- Properties.currentlyRunning.parse(spySeq)
    } addCount(spy.toString)
    true
  }

  def mockSendAndAssertCount(id: ChainedID, expected: Map[ChainedID, Int], state: String): Unit = {
    send(PropertiesCrumb(id, Properties.logMsg -> s"As ${id.toString}"))
    expected.foreach { case (innerId, i) =>
      val actual = counts.getOrElse(innerId.toString, 0)
      assert(
        i == actual,
        s"$state: Wrong count for $innerId, sending=$id expected=$i actual=$actual counts=${counts} interests=${Breadcrumbs.interests}")
    }
  }

  private var sent = Map.empty[ChainedID, Int]

  def incrExpected(is: ChainedID*): Map[ChainedID, Int] = {
    for (i <- is.toSeq) {
      val v0 = sent.getOrElse(i, 0)
      sent = sent + ((i, v0 + 1))
    }
    sent
  }
}
