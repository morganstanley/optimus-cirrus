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
package optimus.graph.diagnostics
import optimus.core.MonitoringBreadcrumbs
import optimus.graph.NodeTask

import scala.jdk.CollectionConverters._
import java.util.concurrent.ConcurrentHashMap

object SingleProfiledTaskData {
  private final val startsToReport = new ConcurrentHashMap[String, Long]()
  private final val timingsToReport = new ConcurrentHashMap[String, Long]()

  // TODO (OPTIMUS-57169): include timings here once we implement them
  def flushDataAndSendCrumb(task: NodeTask): Unit =
    if (!startsToReport.isEmpty) {
      MonitoringBreadcrumbs.sendInstrumentedStartsCrumb(task, startsToReport.asScala.toMap)
      startsToReport.clear()
      timingsToReport.clear()
    }

  def updateStarts(node: String): Unit = startsToReport.compute(node, (_: String, n: Long) => n + 1)

  /** TODO (OPTIMUS-57169): Only called once per node at the end of timing. Revisit once we deal with suspends... */
  def updateTime(node: String, time: Long): Unit = timingsToReport.put(node, time)

  /**
   * Return self time of a given instrumented node (enabled through
   * InstrumentationCmds.profileStartsAndSelfTimeOfThisAndDerivedClasses)
   * @param node
   *   fully qualified node name
   * @return
   *   self time
   */
  def selfTimeOf(node: String): Long = timingsToReport.getOrDefault(node, -1)

  /**
   * Return number of starts of a given instrumented node (enabled through
   * InstrumentationCmds.profileStartsAndSelfTimeOfThisAndDerivedClasses)
   * @param node
   *   fully qualified node name
   * @return
   *   total number of starts
   */
  def startsOf(node: String): Long = startsToReport.getOrDefault(node, -1)
}
