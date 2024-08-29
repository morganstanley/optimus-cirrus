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
package optimus.platform.dsi.protobufutils

import java.util.concurrent.atomic.AtomicBoolean
import msjava.slf4jutils.scalalog.getLogger
import optimus.graph.DiagnosticSettings
import optimus.graph.OGTraceIsolation
import optimus.graph.diagnostics.messages.DALRequestCounter
import optimus.graph.diagnostics.messages.DALRequestEvent
import optimus.graph.diagnostics.messages.DALRequestsInFlightCounter

import scala.collection.concurrent.TrieMap

object ClientRequestTracker {
  private val log = getLogger("ClientRequestTracker")
  private val enabled = new AtomicBoolean(
    DiagnosticSettings.getBoolProperty("optimus.dsi.enableClientRequestTracking", true))

  private val correlationMap = TrieMap.empty[String, Option[DALRequestEvent]]

  def add(context: BatchContext): Unit = {
    if (enabled.get) {
      this.synchronized {
        val requestUuid = context.requestUuid
        if (correlationMap.contains(requestUuid))
          log.warn(s"Duplicate request UUID: $requestUuid")
        else {
          if (OGTraceIsolation.isTracing) {
            val dalEnv = context.clientRequests.headOption
              .flatMap(_.completable.env)
              .map(_.config.runtimeConfig.mode.toLowerCase)
              .getOrElse("none")
            val otherStuff = context.clientRequests.flatMap(_.commands).mkString(";")
            // if provided, find locations (for debugger). Note: need deduplication if batch contains multiple queries
            // at the same source location (ie, in a map)
            val locations = context.clientRequests.flatMap(_.callerDetail).map(_.asClickableSource).toSet.mkString("\n")
            val event =
              DALRequestCounter.report(requestUuid, dalEnv, context.clientRequests.size, locations, otherStuff)
            correlationMap.put(requestUuid, Some(event))
            DALRequestsInFlightCounter.report(requestUuid, correlationMap.size) // must be after event is in map
          } else correlationMap.put(requestUuid, None)
        }
      }
    }
  }

  def markComplete(requestUuid: String): Int = {
    if (enabled.get) {
      this.synchronized {
        correlationMap.remove(requestUuid) match {
          case None => log.warn(s"Attempt to mark complete non-existent request $requestUuid in client request tracker")
          case Some(None) => // not tracing...
          case Some(Some(event)) =>
            DALRequestCounter.publishComplete(event)
            DALRequestsInFlightCounter.report(requestUuid, correlationMap.size)
            log.debug(s"Successfully marked request $requestUuid as completed in client request tracker")
        }
        correlationMap.size
      }
    } else -1
  }

  def getRequestsInFlight: Iterable[String] = {
    if (enabled.get) {
      this.synchronized {
        correlationMap.keys.map(_.toString)
      }
    } else Seq.empty
  }

  def numberOfRequestsInFlight: Int = {
    if (enabled.get)
      this.synchronized { correlationMap.size }
    else
      -1
  }
}
