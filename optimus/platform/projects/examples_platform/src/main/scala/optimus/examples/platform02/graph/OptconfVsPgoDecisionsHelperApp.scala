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
package optimus.examples.platform02.graph

import optimus.config.NodeCacheConfigs
import optimus.config.PerEntityConfigGroup
import optimus.config.RegexEntityConfigGroup

import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes
import optimus.graph.diagnostics.gridprofiler.GridProfiler.ProfilerOutputStrings
import optimus.graph.diagnostics.trace.OGLivePGOObserver.PGODecision
import optimus.platform.entersGraph
import optimus.platform.inputs.GraphInputConfiguration
import optimus.platform.util.Log
import optimus.platform.OptimusApp
import optimus.platform.OptimusAppCmdLine
import optimus.platform.util.json.DefaultJsonMapper
import optimus.utils.ErrorIgnoringFileVisitor
import optimus.utils.app.DelimitedStringOptionHandler

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class OptconfVsPgoDecisionsCmdLine extends OptimusAppCmdLine {
  import org.kohsuke.args4j.Option

  @Option(name = "--pgoDecisionsDir", usage = "dir to use for scanning pgo decision json files", required = true)
  val pgoDecisionDir: String = ""

  @Option(
    name = "--optconfPaths",
    usage = "paths to optconfs that are applied",
    required = true,
    handler = classOf[DelimitedStringOptionHandler])
  val optconfPaths: Seq[String] = Seq.empty
}

object OptconfVsPgoDecisionHelperApp extends OptimusApp[OptconfVsPgoDecisionsCmdLine] with Log {
  lazy val pgoDecisionSuffix = ".json"
  lazy val optconfSuffix = s".${ProfilerOutputStrings.optconfExtension}"
  val mapper = DefaultJsonMapper.legacy

  @entersGraph override def run(): Unit = {
    val optconfDecisions = getOptconfSettings(cmdLine.optconfPaths.head)
    val livePgoDecisions = getAllPGODecisions(cmdLine.pgoDecisionDir)
    log.info(s"total decisions from optconf ${optconfDecisions.size}")
    log.info(s"total live pgo decisions ${livePgoDecisions.size}")

    val namesFromOptconf = optconfDecisions.map(_.name)
    val namesFromLivePgoDecisions = livePgoDecisions.map(_.name)

    val propNamesInOptconfOnly = namesFromOptconf diff namesFromLivePgoDecisions
    val propNamesInLivePGOOnly = namesFromLivePgoDecisions diff namesFromOptconf

    val intersect = namesFromLivePgoDecisions intersect namesFromOptconf
    log.info("Finish analysis")
  }

  private def getAllPGODecisions(dir: String): Seq[PGODecision] = {
    val path = Paths.get(dir)
    val livePGODecisions: mutable.Map[String, PGODecision] = mutable.Map.empty[String, PGODecision]

    Files.walkFileTree(
      path,
      new ErrorIgnoringFileVisitor {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          val f = file.toAbsolutePath.toString
          if (f.endsWith(pgoDecisionSuffix)) {
            log.info(s"found pgo decision file at: ${file.toAbsolutePath.toString}")
            val content = Source.fromFile(f).mkString
            val decisions = mapper.readValue[List[PGODecision]](content).filter(x => !x.name.contains("~"))
            for (d <- decisions) livePGODecisions += (d.name -> d)
          }
          FileVisitResult.CONTINUE
        }
      }
    )

    livePGODecisions.values.toSeq.sortBy(_.name)
  }

  private def getOptconfSettings(optconfPath: String): Seq[PGODecision] = {
    val optconfProvider = NodeCacheConfigs.verifyPathAndCreateProvider(optconfPath)

    optconfProvider
      .map { p =>
        log.info(s"Found optconf at $p")
        GraphInputConfiguration.configureCachePoliciesFromProviders(Seq(p))
        val entityConfigs = NodeCacheConfigs.entityConfigs
        val allConfigs: ArrayBuffer[PGODecision] = ArrayBuffer.empty

        entityConfigs.foreach {
          case entConf: PerEntityConfigGroup =>
            val propConfigs = entConf.entityToConfig
            for ((ent, cfg) <- propConfigs) {
              for ((nodeName, nodeConfig) <- cfg.propertyToConfig) {
                val disablescache = nodeConfig.config.getOrElse(null).disableCache
                val decision = new PGODecision(ent + "." + nodeName, disablescache, false)
                allConfigs += decision
              }
            }
          case _: RegexEntityConfigGroup =>
        }

        allConfigs.sortBy(_.name).toSeq
      }
  }.getOrElse(Seq.empty)
}
