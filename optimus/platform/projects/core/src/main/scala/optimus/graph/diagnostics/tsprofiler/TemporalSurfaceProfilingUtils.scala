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
package optimus.graph.diagnostics.tsprofiler

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import optimus.graph.diagnostics.gridprofiler.GridProfilerUtils
import optimus.platform.inputs.registry.ProcessGraphInputs
import optimus.platform.temporalSurface.tsprofiler.TemporalSurfaceProfilingData
import optimus.platform.temporalSurface.tsprofiler.TemporalSurfaceProfilingDataManager
import optimus.platform.util.Log
import optimus.platform.util.json.DefaultJsonMapper

object TemporalSurfaceProfilingUtils extends Log {
  private val jsonFileExt = "json"

  private[optimus] def dumpTemporalSurfProfilingData(): Unit = {
    ProcessGraphInputs.ProfileTSLevel.currentValueOrThrow() match {
      case TemporalSurfProfilingLevel.BASIC =>
        val tsProfilingData = TemporalSurfaceProfilingDataManager.tsProfiling
        writeTSProfAsJson(tsProfilingData)
      case TemporalSurfProfilingLevel.AGGREGATE =>
        TemporalSurfaceProfilingDataManager.aggregateTSProfiling(null)
        val tsProfilingData = TemporalSurfaceProfilingDataManager.tsProfiling
        writeTSProfAsJson(tsProfilingData)
      case _ =>
    }
  }

  private def generateBaseFileName(): String = {
    val fileStamp = GridProfilerUtils.generateFileName()
    val baseFileName = GridProfilerUtils.outputPath(fileStamp, ProcessGraphInputs.ProfileTSFolder.currentValueOrThrow())
    s"$baseFileName${TemporalSurfaceProfilingDataManager.tsProfilingFilename}"
  }

  private def initializeMapper(): ObjectMapper = {
    val mapper = DefaultJsonMapper.legacy
    mapper.setConfig(mapper.getDeserializationConfig.without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES))
    mapper
  }

  private def writeTSProfAsJson(profData: TemporalSurfaceProfilingData): Unit = {
    val baseFileName = generateBaseFileName()
    val outputFile = s"$baseFileName.$jsonFileExt"
    val mapper = initializeMapper()
    GridProfilerUtils.writeFile(
      outputFile,
      mapper.writerWithDefaultPrettyPrinter().writeValueAsString(profData)
    )
  }
}
