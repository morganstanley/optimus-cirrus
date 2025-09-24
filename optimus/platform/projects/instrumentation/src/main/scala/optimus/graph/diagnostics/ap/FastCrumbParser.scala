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
package optimus.graph.diagnostics.ap

import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.Properties._
import optimus.platform.util.Log
import optimus.scalacompat.collection._

import com.github.plokhotnyuk.jsoniter_scala.core._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import scala.collection.compat._
import scala.collection.immutable.ArraySeq
import scala.util.control.NonFatal
import scala.util.Try

final case class CurrentlyRunningEntry(taskId: String, depth: Int, crumbLevel: Int, vertexId: String)
final case class TimePulse(pulseMap: Map[String, Double], currentlyRunning: Seq[CurrentlyRunningEntry], snapTime: Long)
final case class TimeSample(pulse: TimePulse, engineRoot: String, gsfEngineId: Option[String])
final case class HotspotRow(uuid: String, other: Map[String, String]) {
  override def toString: String = s"$uuid, $other"
  def getBytes: Long = (uuid + other.mkString(",")).getBytes.length
}
final case class AppArgs(uuid: String, mergedAppName: String, argsType: String, argsSeq: Seq[(String, String)])

object CrumbParser extends Log {
  def getMetricRoot(metric: String): String = metric.split("""\.""").headOption.getOrElse("")
  // for frame
  final case class Frame(pSID: String, profCollapsed: String)
  implicit val frameCodec: JsonValueCodec[Frame] = JsonCodecMaker.make
  // for sample
  final case class Stack(pSlf: Long, pTot: Long, pTpe: String, pSID: String)
  final case class Pulse(snapTimeMs: Long, snapPeriod: Long, profStacks: ArraySeq[Stack])
  final case class Sample(pulse: Pulse, engineRoot: String, gsfEngineId: Option[String])
  implicit val pulseCodec: JsonValueCodec[Pulse] = JsonCodecMaker.make
  implicit val stackCodec: JsonValueCodec[Stack] = JsonCodecMaker.make
  implicit val sampleCodec: JsonValueCodec[Sample] = JsonCodecMaker.make
  // for time sample
  private final case class Flatten(pulseMap: Map[String, Double], taskIds: Seq[CurrentlyRunningEntry], snapTime: Long)
  private def flatten(in: JsonReader, prefix: String = ""): Flatten = {
    def isGraphable(name: String): Boolean = metaFromString(getMetricRoot(name)).graphable
    def isObj: Boolean = {
      val nextT = in.nextToken()
      in.rollbackToken()
      nextT == '{'
    }
    def decodeTaskIds(value: String): Seq[CurrentlyRunningEntry] = {
      value
        .replaceAll("\\[", "")
        .replaceAll("]", "")
        .split(",")
        .sliding(4, 4)
        .flatMap { case Array(taskId, depth, crumbLevel, vertexId) =>
          Try(CurrentlyRunningEntry(taskId, depth.toInt, crumbLevel.toInt, vertexId)).toOption
        }
        .toSeq
    }

    var pulseMap = Map.empty[String, Double]
    var taskIds = Seq[CurrentlyRunningEntry]()
    var snapTime = 0L

    while (!in.isNextToken('}')) {
      val t = new String(Array(in.nextToken()))
      in.rollbackToken()
      // end at empty object
      if (t == "}") {
        in.rollbackToken()
        in.skip()
        return Flatten(pulseMap, taskIds, snapTime)
      }

      val key = in.readKeyAsString()
      val curName = if (prefix.isEmpty) key else s"$prefix.$key"
      if (isObj) {
        pulseMap ++= flatten(in, curName).pulseMap
      } else {
        val value = new String(in.readRawValAsBytes()).replaceAll("\"", "")
        try {
          if (curName == Properties.currentlyRunning.name) {
            if (value.length > 23) {
              val entries = decodeTaskIds(value)
              taskIds = entries
            }
          } else {
            val samples = value.toDouble
            if (curName == Properties.snapTimeMs.name)
              snapTime = value.toLong
            else if (isGraphable(curName) && samples > 0) pulseMap += (curName -> samples)
          }
        } catch {
          case NonFatal(e) => // skip non-double values
        }
      }
    }
    Flatten(pulseMap, taskIds, snapTime)
  }
  implicit val timePulseCodec: JsonValueCodec[TimePulse] = new JsonValueCodec[TimePulse] {
    override def decodeValue(in: JsonReader, default: TimePulse): TimePulse = {
      val res = flatten(in)
      TimePulse(res.pulseMap, res.taskIds, res.snapTime)
    }
    override def nullValue: TimePulse = TimePulse(Map.empty, Seq.empty, 0L)
    override def encodeValue(x: TimePulse, out: JsonWriter): Unit = {}
  }
  implicit val timeCodec: JsonValueCodec[TimeSample] = JsonCodecMaker.make

  // for hotspot
  implicit val mapCodec: JsonValueCodec[Map[String, String]] = new JsonValueCodec[Map[String, String]] {
    override def decodeValue(in: JsonReader, default: Map[String, String]): Map[String, String] = {
      val mapBuilder = Map.newBuilder[String, String]
      while (!in.isNextToken('}')) {
        val key = in.readKeyAsString()
        val value = new String(in.readRawValAsBytes()).replaceAll("\"", "")
        mapBuilder += (key -> value)
      }
      mapBuilder.result()
    }
    override def nullValue: Map[String, String] = Map.empty
    override def encodeValue(x: Map[String, String], out: JsonWriter): Unit = {}
  }
  implicit val hotspotCodec: JsonValueCodec[HotspotRow] = new JsonValueCodec[HotspotRow] {
    override def decodeValue(in: JsonReader, default: HotspotRow): HotspotRow = {
      val data: Map[String, String] = readFromArray[Map[String, String]](in.readRawValAsBytes())
      HotspotRow(data.getOrElse("uuid", "uuid not found!"), data)
    }
    override def nullValue: HotspotRow = HotspotRow("", Map.empty)
    override def encodeValue(x: HotspotRow, out: JsonWriter): Unit = {}
  }
  // for appArgs
  implicit val appArgsCodec: JsonValueCodec[AppArgs] = JsonCodecMaker.make

  // not share or reuse any JSONReader, for example reuse same json object across threads
  def safeJsonRead[T](s: String)(implicit codec: JsonValueCodec[T]): Option[T] = try {
    Some(readFromString[T](s))
  } catch {
    case NonFatal(e) =>
      log.warn(e.toString)
      None
  }
}
