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

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import optimus.buildtool.config.AfsNamingConventions
import optimus.platform.OptimusApp
import optimus.platform.entersGraph
import optimus.utils.ErrorIgnoringFileVisitor

import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes
import scala.collection.mutable.ListBuffer
import scala.io.Source

// parses .bat executable files
// the agent paths are the same in Windows and linux startup scripts so it doesn't matter which ones we parse
object StartupScriptsInspectorApp extends OptimusApp[AgentsInspectorAppCmdLine] with AgentAppUtils {
  override def reportName = "agentsInStartupScripts.json"
  private val agentsKeywordInScripts = "SCOPE_AGENT_PATH="
  mapper.registerModule(DefaultScalaModule)

  @entersGraph override def run(): Unit = {
    val root = cmdLine.rootDir
    val path = Paths.get(root)
    val agentsInfoPerScope: ListBuffer[AgentsInfo] = new ListBuffer()
    var totalStartupScripts = 0

    Files.walkFileTree(
      path,
      new ErrorIgnoringFileVisitor {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          val f = file.toAbsolutePath.toString
          if (f.endsWith(".bat")) {
            log.info(s"found .bat file: $f")

            for (line <- Source.fromFile(f).getLines()) {
              if (line.contains(agentsKeywordInScripts)) {
                log.info(line)
                val agentsInAppScript = line.substring(line.indexOf("=") + 1).split(",")
                // just so we can print relevant meta bundle and agent paths for local vs afs
                val startupScriptSubPath =
                  if (f.contains(AfsNamingConventions.AfsDistStr))
                    file.subpath(1, file.getNameCount)
                  else file.subpath(3, file.getNameCount)
                agentsInfoPerScope += AgentsInfo(startupScriptSubPath.toString, agentsInAppScript)
              }
            }
            totalStartupScripts += 1
          }
          FileVisitResult.CONTINUE
        }
      }
    )

    log.info(s"Total number of startup scripts read: $totalStartupScripts")
    val reportPath = reportFullPath(cmdLine.outDir, reportName)

    val agentsToWrite =
      if (cmdLine.includeNoAgentScopes) agentsInfoPerScope
      else
        agentsInfoPerScope.filterNot(_.agents.isEmpty)

    writeAgentsReport(agentsToWrite.sortBy(_.scope), reportPath)
  }
}
