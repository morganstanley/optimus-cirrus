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
package optimus.platform.relational

import java.util.concurrent.atomic.AtomicBoolean

import optimus.graph.DiagnosticSettings

object PriqlSettings {

  /**
   * configures the optimus async concurrency level for in-memory priql operations. This controls how many nodes will be
   * scheduled for execution at the same time (e.g. for map operations) and so limits the peak transient memory usage
   * from queued nodes.
   */
  val concurrencyLevel = java.lang.Integer.getInteger("optimus.priql.concurrency", 100000)

  val enableTableScanCheck = DiagnosticSettings.getBoolProperty("optimus.priql.enableTableScanCheck", true)

  val handlePropertyReadErrorForQueryDisplay =
    DiagnosticSettings.getBoolProperty("optimus.priql.handlePropertyReadErrorForQueryDisplay", true)

  // WARNING : If default value is modified, please change the code DistributionUtils.scala
  val defaultEnableProjectedQueryValue = false
  private val defaultEnableProjectedQuery =
    DiagnosticSettings.getBoolProperty("optimus.priql.projected.enable", defaultEnableProjectedQueryValue)
  private val projectedQuery = new AtomicBoolean(defaultEnableProjectedQuery)
  private[optimus] def setProjectedQuery(b: Boolean) = projectedQuery.set(b)
  private[optimus] def resetProjectedQuery() = projectedQuery.set(defaultEnableProjectedQuery)
  def enableProjectedPriql = projectedQuery.get

  val accIdentifierLength = DiagnosticSettings.getIntProperty("optimus.priql.projected.identifierLength", 64)

  val enableGroupByCollection =
    DiagnosticSettings.getBoolProperty("optimus.priql.projected.enableGroupByCollection", false)

  // This entityDenyList is to store the entity which can't be reduced as projected entity even if it is marked (projected = true).
  private var entityDenyList: Set[String] = getProjectedDenyList
  private def getProjectedDenyList: Set[String] = {
    Option(DiagnosticSettings.getStringProperty("optimus.priql.projected.classNameDenyListForPriqlProjection"))
      .map(_.split(",").toSet)
      .getOrElse(Set.empty[String]) ++ Set(
      "optimus.dsi.base.AccRegistrationCheck"
    ) // this class is for integration test
  }
  private[optimus] def addToProjectedDenyList(clz: Class[_]): Unit = synchronized {
    entityDenyList += clz.getName
  }
  private[optimus] def removeFromProjectedDenyList(clz: Class[_]): Unit = synchronized {
    entityDenyList -= clz.getName
  }
  private[optimus] def resetProjectedDenyList: Unit = synchronized {
    entityDenyList = getProjectedDenyList
  }

  def projectedEntityDenyList: Set[String] = entityDenyList

  // Only enable projected registration metadata check if this flag is true.
  val enableProjectedRegistrationCheck =
    DiagnosticSettings.getBoolProperty("optimus.priql.projected.registrationCheck", false)

  // Full text search
  val defaultEnableFullTextSearchQueryValue = false
  private val fullTextSearchQuery = new AtomicBoolean(defaultEnableFullTextSearchQueryValue)
  private[optimus] def setFullTextSearchQuery(b: Boolean) = fullTextSearchQuery.set(b)
  private[optimus] def resetFullTextSearchQuery() = fullTextSearchQuery.set(defaultEnableFullTextSearchQueryValue)
  def enableFullTextSearchPriql =
    DiagnosticSettings.getBoolProperty("optimus.priql.fullTextSearch.enable", fullTextSearchQuery.get)

}
