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
import scala.io.Source
import optimus.platform.entersGraph
import optimus.platform.util.Log
import optimus.platform.OptimusApp
import optimus.platform.OptimusAppCmdLine
import optimus.utils.ErrorIgnoringFileVisitor

class PGODecisionAnalysis extends OptimusAppCmdLine {
  import org.kohsuke.args4j.Option

  @Option(
    name = "--rootDir",
    aliases = Array("--root"),
    usage = "root dir to start scanning for csv files",
    required = false)
  val rootDir: String = ""
}

object PGODecisionAnalysis extends OptimusApp[PGODecisionAnalysis] with Log {
  @entersGraph override def run(): Unit = {
    val root = cmdLine.rootDir
    val path = Paths.get(root)
    var benefitCounter: Int = 0
    var noHitsCounter: Int = 0
    var ratioCounter: Int = 0
    var totalCounter: Int = 0
    Files.walkFileTree(
      path,
      new ErrorIgnoringFileVisitor {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          log.info(s"Visiting file: ${file.toAbsolutePath.toString}")
          val f = file.toAbsolutePath.toString
          if (f.endsWith("explain_pgo_decision.csv")) {
            log.info(s"Found PGO decision file at: $f")
            val file = Source.fromFile(f)
            val lines = file.getLines()

            val header = lines.next().split("\",\"")
            val benefitIndex = header.indexOf("No Benefit")
            val noHitsIndex = header.indexOf("No Hits")
            val ratioIndex = header.indexOf("Low Hit Ratio")

            while (lines.hasNext) {
              val line = lines.next()
              val lineArr = line.split(",")
              if (lineArr(benefitIndex).toUpperCase.contains("TRUE")) benefitCounter += 1
              if (lineArr(noHitsIndex).toUpperCase.contains("TRUE")) noHitsCounter += 1
              if (lineArr(ratioIndex).toUpperCase.contains("TRUE")) ratioCounter += 1

              totalCounter += 1
            }
            file.close()
          }
          FileVisitResult.CONTINUE
        }
      }
    )
    log.info(
      s"Total cacheable PGO decisions: $totalCounter\nWith benefit: $benefitCounter\nWith no hits: $noHitsCounter\nWith low ratio: $ratioCounter")
  }
}
