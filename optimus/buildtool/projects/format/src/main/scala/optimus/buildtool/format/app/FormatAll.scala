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
package optimus.buildtool.format.app

import java.nio.file.Files
import java.nio.file.Paths

import com.typesafe.config.ConfigFactory
import optimus.buildtool.files.Directory
import optimus.buildtool.format.ModuleFormatterSettings
import optimus.buildtool.format.ObtFileFormatter
import optimus.buildtool.format.WorkspaceStructure

import scala.jdk.CollectionConverters._

object FormatAll extends App {
  val ws = Directory(Paths.get(args.head))
  val wsStructure = WorkspaceStructure.loadAllModulesOrFail(ws)
  val formatter = ObtFileFormatter("  ", ModuleFormatterSettings)
  wsStructure.foreach { module =>
    val contentLines = Files.readAllLines(ws.resolveFile(module.path).path).asScala
    if (contentLines.nonEmpty && contentLines.head.startsWith("//") && contentLines.head.indexOf("format=off") != -1)
      println(s"Skipping ${module.path}")
    else {
      val content = contentLines.mkString("\n")
      val formatted = formatter.formatConfig(ConfigFactory.parseString(content).resolve())
      if (content != formatted) {
        println(s"Formatting ${module.path}")
        Files.write(ws.resolveFile(module.path).path, formatted.linesIterator.toSeq.asJava)
      }
    }
  }
}
