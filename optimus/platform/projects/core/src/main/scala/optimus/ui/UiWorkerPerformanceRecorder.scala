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
package optimus.ui

import optimus.graph.Settings
import spray.json.JsNumber

import java.util.concurrent.atomic.AtomicReference

object UiWorkerPerformanceRecorder {

  private val uiWorkerBreadcrumbsData = new AtomicReference[UiWorkerBreadcrumbs](
    UiWorkerBreadcrumbs(
      totalEvents = 0,
      digestTimeSum = 0,
      threadTimeSum = 0,
      minDigest = Long.MaxValue,
      maxDigest = 0,
      minThreadTime = Long.MaxValue,
      maxThreadTime = 0))

  def recordWorkerPerformance(digestTime: Long, threadInactiveTime: Long): Any = {
    if (Settings.publishThrottledHandlerProfilingStats && Settings.publishUiWorkerStats) {
      uiWorkerBreadcrumbsData.getAndUpdate(prevObj =>
        UiWorkerBreadcrumbs(
          totalEvents = prevObj.totalEvents + 1,
          digestTimeSum = prevObj.digestTimeSum + digestTime,
          threadTimeSum = prevObj.threadTimeSum + threadInactiveTime,
          minDigest = Math.min(prevObj.minDigest, digestTime),
          maxDigest = Math.max(prevObj.maxDigest, digestTime),
          minThreadTime = Math.min(prevObj.minThreadTime, threadInactiveTime),
          maxThreadTime = Math.max(prevObj.maxThreadTime, threadInactiveTime)
        ))
    }
  }

  def getBreadcrumbs: Map[String, JsNumber] = {
    val breadcrumbsData: UiWorkerBreadcrumbs = uiWorkerBreadcrumbsData.getAndSet(
      UiWorkerBreadcrumbs(
        totalEvents = 0,
        digestTimeSum = 0,
        threadTimeSum = 0,
        minDigest = Long.MaxValue,
        maxDigest = 0,
        minThreadTime = Long.MaxValue,
        maxThreadTime = 0))

    if (breadcrumbsData.totalEvents == 0) {
      return Map(
        "avgDigestTime" -> JsNumber(-1),
        "minDigestTime" -> JsNumber(-1),
        "maxDigestTime" -> JsNumber(-1),
        "avgThreadSleepTime" -> JsNumber(-1),
        "minThreadSleepTime" -> JsNumber(-1),
        "maxThreadSleepTime" -> JsNumber(-1),
        "totalEvents" -> JsNumber(-1)
      )
    }

    Map(
      "avgDigestTime" -> JsNumber((breadcrumbsData.digestTimeSum / breadcrumbsData.totalEvents.asInstanceOf[Double])),
      "minDigestTime" -> JsNumber(breadcrumbsData.minDigest),
      "maxDigestTime" -> JsNumber(breadcrumbsData.maxDigest),
      "avgThreadSleepTime" -> JsNumber(
        (breadcrumbsData.threadTimeSum / breadcrumbsData.totalEvents.asInstanceOf[Double])),
      "minThreadSleepTime" -> JsNumber(breadcrumbsData.minThreadTime),
      "maxThreadSleepTime" -> JsNumber(breadcrumbsData.maxThreadTime),
      "totalEvents" -> JsNumber(breadcrumbsData.totalEvents)
    )
  }
}

final case class UiWorkerBreadcrumbs(
    totalEvents: Int,
    digestTimeSum: Long,
    threadTimeSum: Long,
    minDigest: Long,
    maxDigest: Long,
    minThreadTime: Long,
    maxThreadTime: Long)
