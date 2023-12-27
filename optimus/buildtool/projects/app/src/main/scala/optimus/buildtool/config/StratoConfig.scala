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
package optimus.buildtool.config

import optimus.stratosphere.config.CustomWorkspace
import optimus.stratosphere.config.StratoWorkspace
import com.typesafe.config.Config
import msjava.slf4jutils.scalalog
import optimus.buildtool.files.Directory
import optimus.buildtool.files.LocalDirectoryFactory
import optimus.buildtool.trace.LoadStratoConfig
import optimus.buildtool.trace.ObtTrace
import optimus.platform._

// StratoWorkspace does not have proper equals() defined and is mutable so we need to extract required information
final case class StratoConfig(
    scalaVersion: String,
    stratoVersion: String,
    obtVersion: String,
    scalaHome: String,
    javaHome: String,
    config: Config,
    stratoDirs: Seq[Directory]
)

@entity object StratoConfig {
  private val stratoLogger = scalalog.getLogger(StratoWorkspace)

  @node private def watchStratoDir(directoryFactory: LocalDirectoryFactory, dir: Directory): Directory = {
    val d = directoryFactory // this is the directory we're going to watch for OBT config changes
      .lookupDirectory(
        dir.path,
        fileFilter = Directory.fileExtensionPredicate("conf"),
        maxDepth = 1
      )
    d.declareVersionDependence()
    d
  }

  @node def load(directoryFactory: LocalDirectoryFactory, workspaceSrcRoot: Directory): StratoConfig =
    ObtTrace.traceTask(ScopeId.RootScopeId, LoadStratoConfig) {
      // Strato config is loaded from .conf files in src, src/profiles and config dirs
      val stratoDirs = Seq(
        watchStratoDir(directoryFactory, workspaceSrcRoot),
        watchStratoDir(directoryFactory, WorkspaceLayout.Strato.config(workspaceSrcRoot)),
        watchStratoDir(directoryFactory, WorkspaceLayout.Strato.profiles(workspaceSrcRoot))
      )

      val ws = StratoWorkspace(CustomWorkspace(workspaceSrcRoot.parent.path), s => stratoLogger.info(s))

      StratoConfig(
        scalaVersion = ws.scalaVersion,
        stratoVersion = ws.stratosphereVersion,
        obtVersion = ws.obtVersion,
        scalaHome = ws.scalaHomePath,
        javaHome = ws.internal.java.home.toString,
        config = ws.config,
        stratoDirs = stratoDirs
      )
    }
}
