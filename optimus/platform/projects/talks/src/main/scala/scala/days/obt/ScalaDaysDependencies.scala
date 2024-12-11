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
package scala.days.obt

/* import optimus.platform.reactive.DataSource
import optimus.platform.reactive.OutputTick
import optimus.platform.reactive.Target */
import optimus.platform.util.Log
import optimus.platform._

import scala.util.Try

/* class ExternalVersionUpdater(scope: ScopeId, firstVersionToPublish: Int) extends DataSource[Int] with Log {
  private val versions: Iterator[Int] = Stream.from(firstVersionToPublish).iterator

  override def doStart(): Unit = {
    val t = new Thread() {
      override def run(): Unit = {
        var i = 0
        while (i < 3) {
          Thread.sleep(5000)
          val version = versions.next()
          log.info(s"Updating $scope version: $version")
          publishData(version)
          i = i + 1
        }
      }
    }
    t.start()
  }
}


class BuildOutputTarget extends Target[BuildResult] with Log {
  override protected def output(t: Try[BuildResult], meta: OutputTick): Unit = {
    log.info(s"Received build output: ${t.get}")
  }
}
*/

// helper to snap current version number for log display
trait DisplayString { self: BuildResult =>
  override val toString: String = s"BuildResult(${artifacts
      .map { artifact =>
        val id = artifact.scopeId
        val fingerprint = artifact.fingerprint.map { case (id, v) => s"$id -> $v" }.mkString(", ")
        s"${artifact.getClass.getSimpleName}($id, [fingerprint: $fingerprint])"
      }
      .mkString("\n\t", "\n\t", "\n")})\n"
}

// helper to introduce delays to signature and class compile times based on scope id
@entity trait Timings { self: ScopeConfiguration =>
  // simulate time taken for signatures to compile
  val signatureTime: Int = self.id.name match {
    case "utils" => 1
    case "data"  => 1
    case "model" => 1
    case "app1"  => 2
    case "app2"  => 2
  }

  // time taken for classes to compile
  val classesTime: Int = self.id.name match {
    case "utils" => 1
    case "data"  => 2
    case "model" => 3
    case "app1"  => 4
    case "app2"  => 5
  }
}
