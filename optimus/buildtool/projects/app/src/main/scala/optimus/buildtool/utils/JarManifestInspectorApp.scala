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
package optimus.buildtool.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import optimus.buildtool.config.AfsNamingConventions
import optimus.buildtool.files.JarAsset
import optimus.config.InstallPathLocator.PathingJarSuffix
import optimus.platform.OptimusApp
import optimus.platform.OptimusAppCmdLine
import optimus.platform.entersGraph
import optimus.platform.util.Log
import optimus.utils.ErrorIgnoringFileVisitor

import java.io.File
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes
import scala.collection.mutable.ListBuffer

private[utils] final case class AgentsInfo(scope: String, agents: Seq[String])

trait AgentAppUtils extends Log {
  val mapper = new ObjectMapper()
  def reportName: String
  def reportFullPath(outputPath: String, reportName: String): String = {
    val prefix = if (outputPath.endsWith(File.separator)) outputPath else s"$outputPath${File.separator}"
    s"$prefix$reportName"
  }
  def writeAgentsReport(parsedAgents: Seq[AgentsInfo], reportPath: String): Unit = {
    try {
      mapper.writeValue(Paths.get(reportPath).toFile, parsedAgents)
    } catch {
      case ex: Exception =>
        log.error(s"Could not write json agents report to $reportPath", ex)
    }
  }
}

class AgentsInspectorAppCmdLine extends OptimusAppCmdLine {
  import org.kohsuke.args4j.Option

  @Option(
    name = "--rootDir",
    usage = "root dir to start scanning for agents (could be from runtime pathing jar or app startup scripts)",
    required = true)
  val rootDir: String = ""

  @Option(name = "--outputDir", usage = "directory where to write the resulting agent paths", required = false)
  val outDir: String = ""

  @Option(
    name = "--includeScopesWithNoAgents",
    usage = "whether to include in the generated report scopes or apps that have no agents",
    required = false
  )
  val includeNoAgentScopes: Boolean = false
}

object JarManifestInspectorApp extends OptimusApp[AgentsInspectorAppCmdLine] with AgentAppUtils {
  override def reportName = "agentsInManifest.json"
  mapper.registerModule(DefaultScalaModule)

  @entersGraph override def run(): Unit = {
    val root = cmdLine.rootDir
    val path = Paths.get(root)
    val agentsInfoPerScope: ListBuffer[AgentsInfo] = new ListBuffer()
    var totalPathingJars = 0

    Files.walkFileTree(
      path,
      new ErrorIgnoringFileVisitor {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          val f = file.toAbsolutePath.toString
          if (f.endsWith(PathingJarSuffix)) {
            log.info(s"found $PathingJarSuffix file: $f")
            val jarAsset = JarAsset(file.toAbsolutePath)
            val pathingJarManifest = Jars.readManifestJar(jarAsset)
            if (pathingJarManifest.isDefined) {
              val agentsFromManifest = Jars.extractAgentsInManifest(jarAsset, pathingJarManifest.get)
              // just so we can print relevant meta bundle and pathing jar name for local vs afs
              val pathingJarSubPath =
                if (f.contains(AfsNamingConventions.AfsDistStr))
                  file.subpath(1, file.getNameCount)
                else file.subpath(3, file.getNameCount)
              agentsInfoPerScope += AgentsInfo(pathingJarSubPath.toString, agentsFromManifest.map(_.pathString))
              totalPathingJars += 1
            } else {
              log.error(s"Something went wrong. Could not get agents from $PathingJarSuffix for: $f")
            }
          }
          FileVisitResult.CONTINUE
        }
      }
    )

    log.info(s"Total number of pathing jars read: $totalPathingJars")
    val reportPath = reportFullPath(cmdLine.outDir, reportName)

    val agentsToWrite =
      if (cmdLine.includeNoAgentScopes) agentsInfoPerScope
      else
        agentsInfoPerScope.filterNot(_.agents.isEmpty)

    writeAgentsReport(agentsToWrite.sortBy(_.scope), reportPath)
  }
}
