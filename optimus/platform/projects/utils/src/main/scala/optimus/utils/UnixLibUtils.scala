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

package optimus.utils
import optimus.logging.LoggingInfo
import optimus.platform.util.Log

import java.nio.file.Files
import java.nio.file.Paths
import scala.jdk.CollectionConverters._
import scala.util._

object UnixLibUtils extends Log {

  lazy val libraries: Iterable[String] =
    if (LoggingInfo.isWindows) Iterable.empty
    else
      Try {
        val LibMatch = """.*\s(\S*\.so[\.\d]*)$""".r
        Files
          .readAllLines(Paths.get("/proc/self/maps"))
          .asScala
          .collect { case LibMatch(line) =>
            line
          }
          .distinct
      } match {
        case Success(libs) => libs
        case Failure(e) =>
          log.error("Failed to read /proc/self/maps", e)
          Iterable.empty
      }

  def findLibrary(lib: String): Option[String] = libraries.find(_.contains(lib))
}
