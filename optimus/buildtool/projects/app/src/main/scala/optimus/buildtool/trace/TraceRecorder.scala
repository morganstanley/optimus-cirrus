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
package optimus.buildtool.trace

import com.github.plokhotnyuk.jsoniter_scala.core._
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker

import java.nio.file.Files
import java.time.Instant
import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.utils.AssetUtils

import scala.collection.mutable.ArrayBuffer

private[buildtool] object TraceRecorder {
  private val log = getLogger(this.getClass)

  private def toEpocMicro(i: Instant): Long =
    (i.getEpochSecond * 1000000L) + (i.getNano / 1000L)

  final case class TraceResult private[TraceRecorder] (
      name: ScopeId,
      category: CategoryTrace,
      durationSec: Double,
      threadId: Long)

  class TraceRecorderTask private[TraceRecorder] (val scopeId: ScopeId, val category: CategoryTrace, startTime: Instant)
      extends DefaultTaskTrace {
    private val prettyCategory = category.name
    val startTimeMicros: Long = toEpocMicro(startTime)
    def endTimeMicros = endTimeMicrosVar
    @volatile private var endTimeMicrosVar: Long = -1
    private val threadId: Long = Thread.currentThread.threadId
    private val threadName: String = Thread.currentThread.getName

    def durationMicros = {
      val r = endTimeMicros - startTimeMicros
      if (endTimeMicros == -1) log.debug(s"[$scopeId] Incomplete task for $category")
      else if (r < 0) log.debug(s"[$scopeId] Negative time for $category")
      r
    }

    def isDone: Boolean = endTimeMicros > -1

    override def end(success: Boolean, errors: Int, warnings: Int, time: Instant): Unit = synchronized {
      assert(!isDone, s"task was already completed: $this")
      endTimeMicrosVar = toEpocMicro(time)
    }

    def toPrettyString(byThread: Boolean): String = {
      val id = {
        if (category.isAsync) category.categoryName
        else if (byThread) threadId
        else scopeId
      }
      val extras =
        s"""{ "end_in_micros": $endTimeMicros, "thread_id": $threadId, "thread_name": "$threadName" }"""
      s"""{"name": "$scopeId:$prettyCategory", "cat": "$prettyCategory", "ph": "X", "ts": $startTimeMicros, "dur": $durationMicros, "pid": 0, "tid": "$id", "args": $extras }"""
    }

    def result = TraceResult(scopeId, category, durationMicros * 1.0e-6, threadId)

    override def toString: String = s"TraceRecorderTask($scopeId, $category)"
  }

  implicit val CategoryJsonValueCodec: JsonValueCodec[CategoryTrace] = new JsonValueCodec[CategoryTrace] {
    override def decodeValue(in: JsonReader, default: CategoryTrace): CategoryTrace = in.readString(null) match {
      case x if x == Queue.name      => Queue
      case x if x == Signatures.name => Signatures
      case x if x == Outline.name    => Outline
      case x if x == Scala.name      => Scala
      case x if x == Java.name       => Java
      case x                         => throw new MatchError(s"$x doesn't match any defined category traces")
    }

    override def encodeValue(x: CategoryTrace, out: JsonWriter): Unit = out.writeVal(x.name)
    override def nullValue: CategoryTrace = null
  }

  implicit val traceResultJsonValueCodec: JsonValueCodec[List[TraceResult]] = JsonCodecMaker.make

}

class TraceRecorder(traceFilePrefixOpt: Option[Directory] = None) extends DefaultObtTraceListener {
  private val log = getLogger(getClass)
  import TraceRecorder._

  private val traces = ArrayBuffer[TraceRecorderTask]()

  override def startTask(
      scopeId: ScopeId,
      category: CategoryTrace,
      time: Instant = patch.MilliInstant.now()): TraceRecorderTask = {
    val t = new TraceRecorderTask(scopeId, category, time)
    traces.synchronized { traces += t }
    t
  }

  def completedTasks: List[TraceRecorderTask] = traces.synchronized {
    traces.iterator.filter(_.durationMicros >= 0).toList
  }

  override def endBuild(success: Boolean): Boolean = {
    traceFilePrefixOpt.foreach(writeToFiles)
    reset()
    true
  }

  private def writeToFiles(traceFilePrefix: Directory): Unit = {
    val result = completedTasks
    // pre-sorted traces go into traceFilePrefix/traceByXXX.trace
    // full JSON trace goes into traceFilePrefix/traces-MILLIS.json
    Files.createDirectories(traceFilePrefix.path)
    writeTraces(result.map(_.toPrettyString(true)), traceFilePrefix, "traceByThread")
    writeTraces(result.map(_.toPrettyString(false)), traceFilePrefix, "traceByName")
    traceFilePrefix.path.toFile.mkdirs()
    val file = traceFilePrefix.resolveFile(s"traces-${System.currentTimeMillis()}.json").asJson
    AssetUtils.storeJsonAtomically(file, result.map(_.result), replaceIfExists = true, zip = false)
    log.info(s"Wrote trace durations to $file")
  }

  private def writeTraces(traces: List[String], traceFilePrefix: Directory, name: String): Unit = {
    val fname = s"${traceFilePrefix.name}-$name.trace"
    val file = traceFilePrefix.path.resolveSibling(fname)
    val lines = traces.mkString("""{"traceEvents": [""", ", \n", "]}")
    Files.write(file, lines.getBytes())
    log.info(s"Flushed trace data to ${file.toAbsolutePath}")
  }

  private[buildtool] def reset(): Unit = traces.clear()
}
