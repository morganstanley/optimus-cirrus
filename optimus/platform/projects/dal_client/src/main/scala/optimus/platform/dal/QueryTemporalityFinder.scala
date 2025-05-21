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
package optimus.platform.dal

import scala.annotation.nowarn
import optimus.core.CoreAPI
import optimus.platform._
import optimus.platform.NodeFailure
import optimus.platform.NodeSuccess
import optimus.platform.TemporalContext
import optimus.platform.dsi.bitemporal.DSIQueryTemporality
import optimus.platform.temporalSurface.TemporalSurfaceTemporalityException
import optimus.platform.temporalSurface.impl.QueryTemporalityResolver
import optimus.platform.temporalSurface.impl.TemporalContextImpl
import optimus.platform.temporalSurface.operations.QueryTree
import optimus.platform.temporalSurface.operations.TemporalSurfaceQuery
import optimus.platform.temporalSurface.tsprofiler.TemporalSurfaceProfilingDataManager

private[optimus] object QueryTemporalityFinder {
  @node @scenarioIndependent def findQueryTemporality(
      loadContext: TemporalContext,
      tsQuery: TemporalSurfaceQuery): DSIQueryTemporality.At = {
    val currentTsProf = TemporalSurfaceProfilingDataManager.maybeUpdateProfilingTSData(loadContext)
    @nowarn("msg=10500 optimus.core.CoreAPI.nodeResult")
    val res = tsQuery match {
      case qt: QueryTree =>
        QueryTemporalityResolver.resolveQueryTemporality(
          loadContext.asInstanceOf[TemporalContextImpl] :: Nil,
          qt,
          None,
          currentTsProf,
          None)
      case other =>
        CoreAPI.nodeResult {
          loadContext.asInstanceOf[TemporalContextImpl].operationTemporalityFor(tsQuery)
        } match {
          case NodeSuccess(res) => Some(res.asInstanceOf[QueryTemporality.At])
          case NodeFailure(_)   => None
        }
    }
    getTemporality(res, loadContext, tsQuery)
  }
  // the time that a query should be run. This is used if you are only interested in Erefs, rather than the actual data
  @node @scenarioIndependent def findIndexQueryTemporality(
      loadContext: TemporalContext,
      tsQuery: TemporalSurfaceQuery): DSIQueryTemporality.At = {
    val currentTSProf = TemporalSurfaceProfilingDataManager.maybeUpdateProfilingTSData(loadContext)
    @nowarn("msg=10500 optimus.core.CoreAPI.nodeResult")
    val res = tsQuery match {
      case qt: QueryTree =>
        QueryTemporalityResolver.resolveQueryTemporality(
          loadContext.asInstanceOf[TemporalContextImpl] :: Nil,
          qt,
          None,
          currentTSProf,
          None)
      case other =>
        CoreAPI.nodeResult {
          val surface =
            loadContext.asInstanceOf[TemporalContextImpl].findFirstPossiblyMatchingLeaf(tsQuery)
          surface.currentTemporality
        } match {
          case NodeSuccess(res) => Some(res)
          case NodeFailure(_)   => None
        }
    }
    getTemporality(res, loadContext, tsQuery)
  }

  private def getTemporality(
      queryTemporality: Option[QueryTemporality.At],
      loadContext: TemporalContext,
      tsQuery: TemporalSurfaceQuery): DSIQueryTemporality.At =
    if (queryTemporality.isDefined) {
      val t = queryTemporality.get
      DSIQueryTemporality.At(t.validTime, t.txTime)
    } else
      throw new TemporalSurfaceTemporalityException(s"cannot determine temporality for '$tsQuery' in '$loadContext'")
}
