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
package optimus.graph.diagnostics.gridprofiler

import optimus.graph.DiagnosticSettings
import optimus.graph.diagnostics.gridprofiler.GridProfiler.getDefaultLevel
import optimus.graph.diagnostics.gridprofiler.GridProfiler.log
import optimus.graph.diagnostics.gridprofiler.Level.Level
import optimus.graph.diagnostics.pgo.PGOMode
import optimus.platform.inputs.GraphInputConfiguration._
import optimus.platform.inputs.loaders.LoaderSource
import optimus.platform.inputs.registry.ProcessGraphInputs
import optimus.platform.inputs.GraphInputConfiguration.setHotspotsFilter
import optimus.platform.inputs.GraphInputConfiguration.setProfileCsvFolder

object GridProfilerDefaults {

  private[optimus] var defaultCustomMetrics = Array.empty[String]

  // set by OptimusApp, commandline option --config-folder
  private[gridprofiler] var defaultConfigFolder = ""

  private[gridprofiler] var defaultPgoModes = collection.Seq.empty[PGOMode]
  private[gridprofiler] var defaultPgoFolder = ""

  // set by OptimusApp, commandline option --profile-aggregation
  private[optimus] var defaultAggregationType = AggregationType.DEFAULT

  private[optimus] def configurePGOModesFromNames(pgoModes: collection.Seq[String], pgoFolder: String): Unit =
    configurePGOModes(pgoModes.map(PGOMode.fromName), pgoFolder)

  private[optimus] def configurePGOModes(pgoModes: collection.Seq[PGOMode], pgoFolder: String): Unit = {
    require(pgoFolder.nonEmpty)
    defaultPgoModes = pgoModes
    defaultPgoFolder = pgoFolder
    if (getDefaultLevel < Level.HOTSPOTSLIGHT) {
      setTraceMode(Level.HOTSPOTSLIGHT.toString)
      log.info(s"Setting profiling level to ${Level.HOTSPOTSLIGHT}. This overrides JVM option for profiling")
    }
  }

  def autoPGOEnabled(defaultPGOModes: Seq[PGOMode] = defaultPgoModes): Boolean = defaultPGOModes.nonEmpty

  private[optimus] def setNonNodeInputDefaults(
      aggregationType: AggregationType.Value,
      customMetrics: Array[String],
      configOutputDir: String,
      pgoModes: collection.Seq[String] = Nil,
      pgoFolder: String = ""): Unit = {
    if (pgoModes.nonEmpty) configurePGOModesFromNames(pgoModes, pgoFolder)

    defaultConfigFolder = Option(configOutputDir).getOrElse("")

    AggregationType.setDefault(aggregationType)

    if (DiagnosticSettings.initialProfileCustomFilter ne null) {
      if (customMetrics.nonEmpty)
        log.warn("Profile custom metrics specified both on command line and JVM option. Command line ignored!")
      defaultCustomMetrics = DiagnosticSettings.initialProfileCustomFilter
    } else {
      defaultCustomMetrics = customMetrics
    }
  }

  def setDefaults(
      lvl: Level,
      aggregationType: AggregationType.Value,
      dir: String,
      customHotspots: Array[String],
      customMetrics: Array[String],
      configOutputDir: String,
      pgoModes: collection.Seq[String] = Nil,
      pgoFolder: String = ""): Unit = {
    setProfileCsvFolder(dir)
    if (!ProcessGraphInputs.ProfileLevel.currentSource.contains(LoaderSource.SYSTEM_PROPERTIES))
      setTraceMode(lvl.toString)
    else log.warn(s"Keeping old value $getDefaultLevel for profiling since it was loaded via system properties!")
    setNonNodeInputDefaults(aggregationType, customMetrics, configOutputDir, pgoModes, pgoFolder)
    setHotspotsFilter(customHotspots)
  }

  private[optimus] def setDefaultLevel(lvl: String, dir: String, agg: String): Unit =
    setDefaults(
      Level.withName(lvl.toUpperCase),
      Option(agg).map(s => AggregationType.withNameOrDefault(s.toUpperCase)).getOrElse(AggregationType.DEFAULT),
      dir,
      Array.empty[String],
      Array.empty[String],
      dir,
      Nil,
      ""
    )

  def setDefaultCustomMetricsFilter(customMetricsFilter: Array[String]): Unit = {
    this.defaultCustomMetrics = customMetricsFilter
  }

  def reportInvalidFilter(hf: HotspotFilter): Unit = {
    val msg =
      s"""
Invalid hotspot filter specification $hf: filter with the limit of ${hf.ratio * 100}% does not filter anything"
Proceeding with the default hotspot filter "SelfTime Total 95%"
To request no filters, use "--profile-hotspot-filters none"
"""
    log.warn(msg)
  }
  def showHotspotsFiltersUsage(): Unit = {
    val msg =
      s"""
Invalid hotspots filter specification: filter definition must consist of three whitespace-separated tokens: metric, type, and threshold
Supported metrics are: ${Filter.values.mkString(",")}
Supported filter types are: ${FilterType.values.mkString(",")}
Threshold may be specified as a regular float ("0.95") or as percentage ("95%")
Examples: "--profile-hotspot-filters starts total 95%" will report only the hotspots whose starts counters contribute to 95% percentile of all starts
          "--profile-hotspot-filters CacheHits first 0.1" will report only the hotspots taht appear in the first 10% of rows in descending cache hits order
Proceeding with the default hotspot filter "SelfTime Total 95%"
To request no filters, use "--profile-hotspot-filters none"
"""
    log.warn(msg)
  }
}
