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
package optimus.stratosphere.config

import java.util.concurrent.atomic.AtomicReference

/* Workspace manager allows to switch and access workspaces without explicitly passing them in the code.
 * Calling `getWorkspace` returns workspace specified in the closest outer `withWorkspace` block.
 * If there is no outer `withWorkspace` block it returns default set using `setDefaultWorkspace` method.
 * If no default was provided it returns NoWorkspace type of workspace.
 */
class WorkspaceManager() {
  private val defaultWorkspace: AtomicReference[StratoWorkspace] =
    new AtomicReference[StratoWorkspace](StratoWorkspace(NoWorkspace))

  private val workspace: InheritableThreadLocal[StratoWorkspace] = new InheritableThreadLocal[StratoWorkspace]()

  def getWorkspace(): StratoWorkspace = Option(workspace.get()).getOrElse(defaultWorkspace.get())

  def setDefaultWorkspace(newWorkspace: StratoWorkspace): Unit = defaultWorkspace.set(newWorkspace)

  def withWorkspace[T](newWs: StratoWorkspace)(code: => T): T = {
    val previousWorkspace = getWorkspace()
    try {
      workspace.set(newWs)
      previousWorkspace.log.debug(s"Switching workspace to ${newWs.directoryStructure.stratosphereWorkspaceDir}")
      newWs.log.debug(s"Switched from workspace ${previousWorkspace.directoryStructure.stratosphereWorkspaceDir}")
      code
    } finally {
      workspace.set(previousWorkspace)
      newWs.log.debug(s"Returning to workspace ${previousWorkspace.directoryStructure.stratosphereWorkspaceDir}")
      previousWorkspace.log.debug(s"Returned from workspace ${newWs.directoryStructure.stratosphereWorkspaceDir}")
    }
  }
}

object WorkspaceManager {
  // Testing only
  def createTempWorkspace(namePrefix: String): StratoWorkspace = {
    import java.nio.file.Files
    val path = Files.createTempDirectory(namePrefix)
    path.toFile.deleteOnExit()
    new StratoWorkspace(CustomWorkspace(path))
  }
}
