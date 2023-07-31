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
package optimus.stratosphere.utils

import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.filesanddirs.PathsOpts._
import optimus.stratosphere.logger.Logger

object IntellijUtils {
  def wipeOutProjectStructure()(implicit stratoWorkspace: StratoWorkspaceCommon, log: Logger): Unit = {
    val ideaDir = stratoWorkspace.intellijDirectoryStructure.ideaConfigurationStore
    val backupDir = stratoWorkspace.intellijDirectoryStructure.intellijBackups
    log.debug("Creating .idea backup")
    backupDir.dir.create()
    log.debug(s"$backupDir dir exists: ${backupDir.exists()}")
    log.debug(s"$ideaDir dir exists: ${backupDir.exists()}")
    if (ideaDir.exists()) {
      val backupName = ideaDir.name + DateTimeUtils.nextDateTimeSuffix()
      log.debug(s"""Moving backups to $backupDir\\$backupName""")
      ideaDir.moveTo(backupDir).rename(backupName)
      ideaDir.deleteIfExists()
      // delete all backups but the last 5 ones
      backupDir.dir.rotate(keep = 5)
    }
  }
}
