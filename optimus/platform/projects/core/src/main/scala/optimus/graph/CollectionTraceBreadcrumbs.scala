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
package optimus.graph

import java.util
import java.lang.{Long => JLong, String => JString}

import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb.Source
import optimus.breadcrumbs.crumbs.EventCrumb
import optimus.breadcrumbs.crumbs.{Properties => BCProps}
import optimus.breadcrumbs.crumbs.LogPropertiesCrumb
import optimus.breadcrumbs.crumbs.EventCrumb.EventDEPRECATED

import scala.jdk.CollectionConverters._

/**
 * In Splunk, we can use below query to get the total number of collection methods: CTrace | stats sum(payload.countNum)
 * AS total_count by payload.methodName | sort total_count desc
 *
 * If we want to get the count for one application, we can query the uuids by below command, and use the above query
 * with the special uuid: CTrace | stats count(payload.uuid) AS message_num by payload.uuid CTrace AND
 * payload.uuid="..." | stats sum(payload.countNum) AS total_count by payload.methodName | sort total_count desc
 */
object CollectionTraceBreadcrumbs {
  object CollectionTraceSource extends Source { override val name = "CTrace" }
  object CollectionInvokerSource extends Source { override val name = "CInvoke" }
  object CollectionCreationSource extends Source { override val name = "CInit" }

  def publishInfoIfNeeded(uuid: ChainedID, event: EventDEPRECATED): Unit = {
    if (DiagnosticSettings.collectionTraceEnabled) {
      val traced = CollectionTraceSupport.getStatisticAndReset()
      val invoked = CollectionTraceSupport.getCallerStaticAndReset()
      val inited = CollectionTraceSupport.getCtorStaticAndReset()
      Breadcrumbs.info(uuid, EventCrumb(_, CollectionTraceSource, event))
      publishImpl(traced, uuid, CollectionTraceSource)
      publishImpl(invoked, uuid, CollectionInvokerSource)
      publishImpl(inited, uuid, CollectionCreationSource)

      Breadcrumbs.flush()
    }
  }

  private def publishImpl(result: util.HashMap[JString, JLong], uuid: ChainedID, src: Source): Unit = {
    result.asScala foreach { case (k, v) =>
      Breadcrumbs.info(uuid, LogPropertiesCrumb(_, src, BCProps.methodName -> k, BCProps.countNum -> v))
    }
  }
}
