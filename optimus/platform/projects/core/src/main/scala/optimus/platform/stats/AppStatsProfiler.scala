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
package optimus.platform.stats

import java.text.SimpleDateFormat
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.graph.diagnostics.pgo.Profiler

/**
 * MARKER class
 *
 * Since the Profiler class is a Singleton, things get weird - can't MATCH against it, can't do isInstanceOf[Profiler],
 * etc, etc, etc.
 *
 * Also, the caller will not be able to add it to the AnyRef* list in the jsonSegment call.
 *
 * Since Profiler is accessed from some very low-level agent code, do not want to change its structure in any way.
 *
 * Hence this marker class - add one of these to the AnyRef* list to pull data from Profiler.
 *
 * NOTE: If anyone can come up with a cleaner mechanism, feel free to improve this!
 */
class AppStatsProfilerNative {}

/**
 * MARKER class - to invoke Profiler.getProfileData at the time the JSON is produced
 *
 * Some apps may want to use the AppStats.jsonAdd(...) method to define data to be produced when the final JSON is
 * created.
 *
 * Calling Profiler.getProfileData will add a list of TraceTags which exist at the time of that call, but any new
 * TraceTags created by Profiler after the call will not be seen.
 *
 * This marker may be used so that at the time the JSON is produced, the trait will call Profiler.getProfileData at that
 * time to get an up to date list.
 */
class AppStatsProfilerExpanded {}

/**
 * Trait to incorporate Profiler and/or TraceTags from the optimus.graph.diagnostics.Profiler
 *
 * Does NOT accumulate internal information (probeStart...probeEnd are ignored)
 *
 * But DOES override the jsonSegment(...) method to produce an Array of Profiler information
 *
 * If the AnyRef* options contain an AppStatsProfilerMarker instance, then a condensed array is produced in which the
 * first row consists of the headers, and all the other rows contain data (all comma delimited)
 *
 * If the AnyRef* options contain an Iterable[TraceTag] instance, then an array is created which contains a sub-object
 * for each data row - with a "label":value entry for each data item
 */
trait AppStatsProfiler extends AppStats {
  val dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS");
  val formatTimeOnly = new SimpleDateFormat("HH:mm:ss.SSS")
  val nameProfiler = "profiler"
  val nameTraceTag = "tracetag"
  var cntProfiler = 0 // In case multiple 'optional' contain TraceTags
  var cntTraceTag = 0

  /** Looks for an Iterable[TraceTag] and processes the TraceTag entries - others ignored */
  override def jsonSegment(optional: AnyRef*): String = {
    cntProfiler = 0
    cntTraceTag = 0
    val sb = new StringBuilder(2048)
    if (optional != null) {
      optional.map { x =>
        {
          x match {
            case it: Iterable[_]               => putExpanded(sb, it)
            case exp: AppStatsProfilerExpanded => putExpanded(sb, Profiler.getProfileData())
            case prof: AppStatsProfilerNative => {
              // Gen the data into a buffer, split on \n, create the JSON
              val wtr = new java.io.CharArrayWriter(512 * 1024) // Profiler data gets big!
              Profiler.writeProfileData(wtr)
              val str = wtr.toString.trim
              if (!str.isEmpty) {
                val rows = wtr.toString.split("\n")
                if (rows.length > 0) {
                  sb.append(quoted(nameProfiler + AppStatsControl.AppStatsArrayMarker + cntProfiler)).append(":[")
                  for (row <- rows) sb.append(quoted(row)).append(',')
                  trimEnd(sb, ',')
                  sb.append("],")
                  cntProfiler += 1
                }
              }
              wtr.close
            }
            case _ => {} // Ignore anything else
          }
        }
      }
    }
    super.jsonSegment(optional: _*) + (if (sb.length == 0) "" else sb.toString)
  }
  // Warning: Must check that the Iterable actually contains TraceTag entries!
  // Because of type erasure, all we really know is that it is an Iterable
  private def putExpanded(sb: StringBuilder, it: Iterable[_]): Unit = {
    if (!it.isEmpty) {
      var isFirst = true
      it.map(y => {
        y match {
          case tt: PNodeTaskInfo => {
            if (isFirst) {
              // This may actually appear to be an array if multiple profilers
              sb.append(quoted(nameTraceTag + AppStatsControl.AppStatsArrayMarker + cntTraceTag)).append(":[")
              isFirst = false
            }
            putTraceTag(sb, tt)
          }
          case _ => {} // Ignore anything else
        }
      })
      if (!isFirst) {
        trimEnd(sb, ',')
        sb.append("],")
        cntTraceTag += 1
      }
    }
  }
  val nanosToMillis = 1000000

  // Put out one entry w trailing ,
  private def putTraceTag(sb: StringBuilder, tt: PNodeTaskInfo): Unit = {
    sb.append('{')
    jsonItem(sb, "ID", tt.id)
    jsonItem(sb, "PName", tt.name)
    jsonItem(sb, "EName", tt.pkgName)
    jsonItem(sb, "start", tt.start)
    jsonItem(sb, "evicted", tt.evicted)
    /*
    jsonItem(sb, "age", tt.age)
    jsonItem(sb, "pia_h", tt.putIfAbsentHit)
    jsonItem(sb, "pia_m", tt.putIfAbsentMiss)
    jsonItem(sb, "pia_time", tt.putIfAbsentTime / nanosToMillis)
    jsonItem(sb, "time", tt.totalTime / nanosToMillis)
    jsonItem(sb, "child_time", tt.totalChildTime / nanosToMillis)
    jsonItem(sb, "self_time", tt.selfTime / nanosToMillis)
    jsonItem(sb, "wall_self_time", (tt.totalTime - tt.totalChildTime) / nanosToMillis)
    jsonItem(sb, "cacheMem", tt.cacheMemoryEffect)
    jsonItem(sb, "gcNativeMem", tt.gcNativeEffect)
    jsonItem(sb, "flags", "" PropertyInfoFlags.toString(tt.flags) ) // disabled for now until merge of optimus
    jsonItem(sb, "recommend", tt.recommend)*/
    jsonItem(sb, "pname", tt.name)
    jsonItem(sb, "clsname", tt.pkgName)
    jsonItem(sb, "rtype", tt.returnType)

    //    if (tt.engineSentTimestamp > 0) {
    //      jsonItem(sb, "distJobName", tt.distJobName)
    //      jsonItem(sb, "engineSentTimestamp", if (tt.engineSentTimestamp > 0) dateFormat.format(new Date(tt.engineSentTimestamp)) else "")
    //      jsonItem(sb, "engineReturnedTimestamp", if (tt.engineReturnedTimestamp > 0) dateFormat.format(new Date(tt.engineReturnedTimestamp)) else "")
    //      jsonItem(sb, "engineStartTimestamp", if (tt.engineStartTimestamp > 0) dateFormat.format(new Date(tt.engineStartTimestamp)) else "")
    //      jsonItem(sb, "engineFinishTimestamp", if (tt.engineFinishTimestamp > 0) dateFormat.format(new Date(tt.engineFinishTimestamp)) else "")
    //      jsonItem(sb, "distribution_time", tt.distributionTime)
    //      jsonItem(sb, "engineID", tt.engineID)
    //    }

    trimEnd(sb, ',')
    sb.append("},")
  }
}
