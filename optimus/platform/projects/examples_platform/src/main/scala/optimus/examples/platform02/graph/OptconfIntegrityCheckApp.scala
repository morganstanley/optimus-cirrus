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

import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes
import optimus.graph.diagnostics.gridprofiler.GridProfiler.ProfilerOutputStrings
import optimus.platform.entersGraph
import optimus.platform.inputs.GraphInputConfiguration
import optimus.platform.util.Log
import optimus.platform.OptimusApp
import optimus.platform.OptimusAppCmdLine
import optimus.utils.ErrorIgnoringFileVisitor

class OptconfIntegrityCheckCmdLine extends OptimusAppCmdLine {
  import org.kohsuke.args4j.Option

  @Option(
    name = "--rootDir",
    aliases = Array("--root"),
    usage = "root dir to start scanning for optconf files",
    required = false)
  val rootDir: String = ""
}

object OptconfIntegrityCheckApp extends OptimusApp[OptconfIntegrityCheckCmdLine] with Log {
  lazy val optconfSuffix = s".${ProfilerOutputStrings.optconfExtension}"

  @entersGraph override def run(): Unit = {
    val root = cmdLine.rootDir
    val path = Paths.get(root)
    Files.walkFileTree(
      path,
      new ErrorIgnoringFileVisitor {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          val f = file.toAbsolutePath.toString
          if (f.endsWith(optconfSuffix)) {
            log.info(s"found optconf file at: ${file.toAbsolutePath.toString}")
            GraphInputConfiguration.configureCachePolicies(Seq(f))
          }
          FileVisitResult.CONTINUE
        }
      }
    )
  }
}
