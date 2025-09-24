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
package optimus.profiler

import optimus.graph.diagnostics.trace.OGTraceMode
import optimus.graph.diagnostics.trace.OGLivePGOObserver._
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import optimus.platform.util.Log

class CycleAnalysisAppCmdLine {
  import org.kohsuke.args4j.Option

  @Option(name = "-cycleFile", aliases = Array("--cycleFile"), usage = "file containing all cycle based data")
  val cycleDataFile: String = ""
}

final case class PgoDecision(cycle: Int, disablesCache: Boolean, disablesXSFT: Option[Boolean])

object CycleAnalysisApp extends App with Log {
  private lazy val cmdLine = new CycleAnalysisAppCmdLine
  private lazy val parser = new CmdLineParser(cmdLine)

  try {
    parser.parseArgument(args.toArray: _*)
  } catch {
    case _: CmdLineException =>
      parser.printUsage(System.err)
      System.exit(1)
  }

  val observer = OGTraceMode.livePGO
  val cycleData = OGTraceMode.livePGO.loadFromFile(cmdLine.cycleDataFile)

  var totalGiven = 0

  for (d <- cycleData if d != null && d.isCacheable && !d.getName.contains("~")) {
    val isGiven = d.statsForGiven()
    val disableCache = if (isGiven) disablesCacheForGiven(d) else disablesCache(d)
    val disableXSFT = if (isGiven) None else Some(disableCache || disablesXSFT(d))

    if (isGiven) {
      totalGiven += 1
      // logHitsAndStarts(d)
    }

    val dc = PgoDecision(d.getCycle, disableCache, disableXSFT)

    var currentData = d.getPrevCycleData

    while (
      currentData != null && currentData.getCycle >= livePGOIgnoreFirstCycles /* && currentData.getStarts > livePGOMinStarts */
    ) {
      val isGivenNode = currentData.statsForGiven()
      val wouldDisableCache = if (isGivenNode) disablesCacheForGiven(currentData) else disablesCache(currentData)
      val wouldDisableXSFT = if (isGivenNode) None else Some(disablesXSFT(currentData))

      val decision = PgoDecision(currentData.getCycle, wouldDisableCache, wouldDisableXSFT)

      if (isGivenNode && (dc.disablesCache != decision.disablesCache)) {
        log.info(s"--- ${currentData.getName} changed decision: $dc and $decision")
//        logHitsAndStarts(d)
//        logHitsAndStarts(currentData)

        currentData = null // stop here when they disagree
      } else currentData = currentData.getPrevCycleData
    }
  }

  log.info(s"Total nb of given captured $totalGiven")

  private def logHitsAndStarts(d: Data): Unit = {
    log.info(s"${d.getName} hits: ${d.getHits} starts: ${d.getStarts}")
  }
}
