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
package optimus.platform.breadcrumbs

import optimus.breadcrumbs.crumbs.Crumb

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

trait ForensicSource extends Crumb.Source {
  override val name: String = "BB"
  override def filterable = false
  override val maxCrumbs: Int = 50
}
object ForensicSource extends ForensicSource

object ControlledSource {
  private final case class KafkaData(
      t: Long,
      rate: Double,
      bytes: Long,
      throttled: Int,
      sent: Int,
      failures: Int,
      wasNotThrottled: Boolean)

  val rateIntervalMs: Double = System.getProperty("optimus.breadcrumbs.rate.interval.sec", "60").toDouble * 1000.0
  val defaultMaxBytesPerInterval: Double =
    System.getProperty("optimus.breadcrumbs.max.kb.per.minutes", "10000").toDouble * 1000.0
  val rateThrottleDryRun: Boolean = !System.getProperty("optimus.breadcrumbs.throttle.enabled", "false").toBoolean
}

trait ControlledSource {
  self: Crumb.Source =>
  import ControlledSource._
  // This trait exists so that we can make the following method private to optimus.platform.
  // Really, optimus.breadcrumbs ought to be in package platform, but that's a pretty big PR.
  private[platform] def filterable: Boolean = true
  final def isFilterable: Boolean = filterable

  private[platform] val maxBytesPerInterval$ : Double = defaultMaxBytesPerInterval
  final def maxBytesPerInterval: Double = maxBytesPerInterval$

  private val kafkaData: AtomicReference[KafkaData] = new AtomicReference(KafkaData(0, 0, 0, 0, 0, 0, true))
  final val kafkaFailures = new AtomicInteger(0)

  final def kafkaSentCount: Int = kafkaData.get.sent

  // accumulate stats and return false if throttled
  private[platform] def anaylyzeKafkaBlob$(s: String): Boolean =
    kafkaData.updateAndGet { prev =>
      // All this gets calculated/reported whether or not this is a dry run
      val t = System.currentTimeMillis()
      val decayedRate = prev.rate * Math.exp(-(t - prev.t) / rateIntervalMs)
      val len = s.length
      val ok = decayedRate + len < maxBytesPerInterval
      val newRate = if (ok) decayedRate + len else decayedRate
      val newCount = if (ok) prev.sent + 1 else prev.sent
      prev.copy(
        t = t,
        bytes = prev.bytes + len,
        throttled = if (ok) prev.throttled else prev.throttled + 1,
        sent = newCount,
        rate = newRate,
        wasNotThrottled = ok
      )
    }.wasNotThrottled || rateThrottleDryRun

  final def analyzeKafkaBlob(s: String): Boolean = anaylyzeKafkaBlob$(s)

  private[platform] def countAnalysisMap: Map[String, Double] = {
    val kd = kafkaData.get
    Map(
      "kmbytes" -> kd.bytes.toDouble / (1000 * 1000),
      "kthrottle" -> kd.throttled.toDouble,
      "kfail" -> kafkaFailures.get.toDouble,
      "ksent" -> kd.sent.toDouble,
      "sent" -> getGenericSendCount.toDouble,
      "qovfl" -> getEnqueueFailures.toDouble
    )
  }
  final def countAnalysis: Map[String, Double] = countAnalysisMap

  private[platform] def snapAnalysisMap: Map[String, Double] = Map(
    "kratembmn" -> (kafkaData.get.rate * (60000.0 / rateIntervalMs) / (1000 * 1000)))

  final def snapAnalysis: Map[String, Double] = snapAnalysisMap

}
