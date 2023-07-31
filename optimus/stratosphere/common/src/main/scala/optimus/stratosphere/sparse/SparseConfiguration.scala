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
package optimus.stratosphere.sparse

import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.filesanddirs.PathsOpts._
import optimus.stratosphere.sparse.SparseConfiguration.currentProfileKey
import optimus.stratosphere.sparse.SparseConfiguration.isEnabledKey
import optimus.stratosphere.utils.ConfigUtils

import java.nio.file.Path
import scala.collection.immutable.Seq

final case class SparseConfiguration(isEnabled: Boolean, profile: Option[SparseProfile]) {
  def save()(implicit ws: StratoWorkspaceCommon): SparseConfiguration = {
    val sparseConfFile: Path = ws.directoryStructure.sparseProfilesConfFile

    profile match {
      case Some(currentProfile) =>
        ConfigUtils.writeConfigProperties(sparseConfFile)(
          currentProfileKey -> currentProfile.name,
          isEnabledKey -> isEnabled)
      case _ =>
        ConfigUtils.updateConfigProperty(sparseConfFile)(isEnabledKey, isEnabled)
    }

    profile.foreach(profile => if (profile.isCustomProfile) profile.save())

    this
  }
}

object SparseConfiguration {

  protected val currentProfileKey = "currentProfile"
  protected val isEnabledKey = "isEnabled"

  private def migrateOldSparseConfig(isEnabled: Boolean)(implicit ws: StratoWorkspaceCommon): Unit = {
    val sparseConfFile: Path = ws.directoryStructure.sparseProfilesConfFile
    val profile = SparseProfile.loadFromPath(sparseConfFile)
    val subProfiles = profile.map(_.subProfiles).getOrElse(Set.empty)
    val scopes = profile.map(_.scopes).getOrElse(Set.empty)

    if (subProfiles.nonEmpty || scopes.nonEmpty) {
      val profile = (subProfiles.toSeq, scopes.toSeq) match {
        case (Seq(singleProfile), Seq()) => SparseProfile.load(singleProfile)
        case _                           => Some(SparseProfile.custom(scopes, subProfiles))
      }

      SparseConfiguration(isEnabled = isEnabled, profile = profile).save()

      import SparseProfile._
      ConfigUtils.removeConfigProperties(sparseConfFile)(profilesKey, scopesKey)
    }
  }

  def load()(implicit ws: StratoWorkspaceCommon): SparseConfiguration = {
    val sparseConfFile = ws.directoryStructure.sparseProfilesConfFile

    if (sparseConfFile.exists()) {
      val isEnabled = ConfigUtils.readWithDefault[Boolean](sparseConfFile)(isEnabledKey, false)
      migrateOldSparseConfig(isEnabled)
      val profileName = ConfigUtils.readConfig[Option[String]](sparseConfFile)(currentProfileKey)
      SparseConfiguration(isEnabled, profileName.flatMap(SparseProfile.load))
    } else {
      SparseConfiguration(isEnabled = false, None)
    }
  }
}
