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
package optimus.profiler.utils
import one.jfr._
import one.jfr.event._

import java.nio.charset.StandardCharsets
import scala.collection.mutable

/**
 * Build list of hotspots for async jfr output.
 */
object JFRTop extends App {

  def getMethodName(method: MethodRef, jfr: JfrReader) = {
    val cls = jfr.classes.get(method.cls)
    val className = jfr.symbols.get(cls.name)
    val methodName = jfr.symbols.get(method.name)
    var result: String = null
    if (className == null || className.length == 0) result = new String(methodName, StandardCharsets.UTF_8)
    else {
      val classStr = new String(className, StandardCharsets.UTF_8)
      val methodStr = new String(methodName, StandardCharsets.UTF_8)
      result = classStr + '.' + methodStr
    }
    result
  }

  def getMethodName(methodId: Long, methodNames: Dictionary[String], jfr: JfrReader): String = {
    var result = methodNames.get(methodId)
    if (result != null) return result
    val method = jfr.methods.get(methodId)
    if (method == null) result = "unknown"
    else result = getMethodName(method, jfr)
    methodNames.put(methodId, result)
    result
  }
  def top(
      jfr: JfrReader,
      n: Int,
      tMin: Long = Long.MinValue,
      tMax: Long = Long.MaxValue,
      absStartTime: Long = 0): Map[String, Long] = {
    val methodNames = new Dictionary[String]
    val agg = new EventAggregator(false, 0.0)
    val eventClass = classOf[ExecutionSample]
    var event: Event = jfr.readEvent(eventClass)
    var t0 = Long.MaxValue
    while (event ne null) {
      agg.collect(event)
      if (event.time < t0) t0 = event.time
      event = jfr.readEvent(eventClass)
    }
    t0 -= absStartTime
    val counts = mutable.HashMap.empty[Long, Long]

    agg.forEach {
      case (event: Event, samples: Long, value: Long) => {
        val t = event.time - t0
        val stackTrace = jfr.stackTraces.get(event.stackTraceId)
        if ((stackTrace ne null) && t >= tMin && (t <= tMax || tMax <= 0)) {
          val methods = stackTrace.methods
          for {
            methodId <- methods
          } {
            val prev = counts.getOrElse(methodId, 0L)
            counts.put(methodId, prev + value * samples)
          }
        }
      }
    }

    counts.toSeq
      .sortBy(c => -c._2)
      .take(n)
      .iterator
      .map { case (mid, c) =>
        val methodName = getMethodName(mid, methodNames, jfr)
        methodName -> c
      }
      .toMap

  }

  // App is for illustration purposes only.
  if (args.length != 4) {
    System.out.println("Usage: n tMin tMax fname");
  }
  val n = args(0).toInt
  val tMin = args(1).toLong * 1000000
  val tMax = args(2).toLong * 1000000
  val fname = args(3)
  val jfr = new JfrReader(fname)
  val tops = top(jfr, n, tMin, tMax)
  println(tops.mkString("\n"))

}
