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
package optimus.platform.tests.common

import optimus.platform.util.Log

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import scala.util._

object TestMetadata extends Log {
  val artifactDirectoryProperty = "optimus.test.metadata.artifacts"

  val TestRunType = "test.run.type"
  val TestRunLive = "live"
  val TestRunCached = "cached"
  val TestRunCacheMissReason = "test.cache.miss.reason"
  val TestRunDuration = "test.run.duration"
  val TestDataLocation = "test.run.output"
  val CacheMode = "test.cache.mode"

  val runningFromIntelliJ: Boolean =
    System.getProperty("sun.java.command").contains("com.intellij.rt.junit.JUnitStarter")

  val maybeRoot: Option[Path] =
    sys.props.get(artifactDirectoryProperty).filter(_.trim.nonEmpty).map(Paths.get(_))
  maybeRoot.foreach { f =>
    assert(Files.exists(f), s"$f does not exist")
  }

  val regressionName: Option[String] = {
    Try {
      System.getenv("TEST_NAME")
    } match {
      case Success(regressionName) => Some(regressionName)
      case Failure(ex) =>
        log.warn(s"Could not retrieve 'regression name' from system environment variable: $ex")
        None
    }
  }

  val moduleGroupName: Option[String] = {
    Try {
      System.getenv("MODULE_GROUP_NAME")
    } match {
      case Success(moduleGroupName) => Option(moduleGroupName)
      case Failure(ex) =>
        log.warn(s"Could not retrieve 'module group name' from system environment variable: $ex")
        None
    }
  }

  val moduleName: Option[String] = {
    Try {
      System.getenv("MODULE_NAME")
    } match {
      case Success(moduleName) => Option(moduleName)
      case Failure(ex) =>
        log.warn(s"Could not retrieve 'module name' from system environment variable: $ex")
        None
    }
  }

  val maybeProjectName: Option[String] =
    sys.props
      .get("module.name")
      .filter(_.nonEmpty)

  val projectName: String = {
    maybeProjectName
      .getOrElse {
        val userDir = System.getProperty("user.dir")
        Paths.get(userDir).getFileName.toString
      }
  }
}
