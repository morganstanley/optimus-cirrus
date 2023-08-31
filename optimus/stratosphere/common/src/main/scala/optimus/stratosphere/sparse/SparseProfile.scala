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

import optimus.stratosphere.bootstrap.StratosphereException
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.filesanddirs.PathsOpts._
import optimus.stratosphere.utils.ConfigUtils

import java.nio.file.Path
import java.nio.file.Paths
import java.security.InvalidParameterException
import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

final case class SparseProfile(name: String, scopes: Set[String], subProfiles: Set[String]) {
  import SparseProfile._

  def isCustomProfile: Boolean = name == customProfileName

  def save()(implicit ws: StratoWorkspaceCommon): Unit = {
    val path =
      if (isCustomProfile) ws.directoryStructure.localSparseProfileConfFile
      else ws.directoryStructure.sparseProfile(name)

    ConfigUtils.writeConfigProperties(path)(
      profilesKey -> subProfiles.toIndexedSeq.sorted.asJava,
      scopesKey -> scopes.toIndexedSeq.sorted.asJava
    )
  }

  def listAllRelativeDirs()(implicit ws: StratoWorkspaceCommon): Set[Path] = {
    val allScopes = scopes ++ loadScopes(subProfiles)
    val keepOpenDirs = ws.internal.sparse.keepOpenDirs

    allScopes.flatMap { scopeName =>
      try {
        val scopeSplitted = scopeName.split('.').filterNot(_.isEmpty)
        val maxScopeDepth = ws.internal.sparse.maxScopeDepth

        if (scopeSplitted.length > maxScopeDepth) {
          throw new InvalidParameterException(s"""Invalid scope length: ${scopeSplitted.length}
                                                 |Max scope length is $maxScopeDepth""".stripMargin)
        }

        val dirPath = scopeSplitted.mkString("/")
        val projectDirPathOpt =
          if (scopeSplitted.length > 1)
            Some(scopeSplitted.patch(from = 2, Seq("projects"), replaced = 0).mkString("/"))
          else None

        (Seq(dirPath) ++ projectDirPathOpt).map(path => Paths.get(path))
      } catch {
        case NonFatal(ex) =>
          throw new StratosphereException(s"Scope $scopeName does not have valid format! Ignoring it.", ex)
      }
    } ++ keepOpenDirs
  }

  protected def loadScopes(profiles: Set[String])(implicit ws: StratoWorkspaceCommon): Set[String] = {
    def loadInner(profileNames: Set[String], seenSoFar: Set[String]): Set[String] =
      profileNames.flatMap { profileConfigName =>
        val profileConfigFile = ws.directoryStructure.sparseProfile(profileConfigName)
        if (!profileConfigFile.exists()) {
          throw new StratosphereException(
            s"""Profile named '$profileConfigName' was not found. Config file is missing: $profileConfigFile.
               |Available profiles: ${SparseProfile.loadAll().map(_.name).mkString(", ")}""".stripMargin)
        }
        if (seenSoFar.contains(profileConfigName)) {
          throw new StratosphereException(
            s"""Profile '$profileConfigName' depends on itself which creates an infinite loop when loading.
               |Please check the configuration file at: $profileConfigFile""".stripMargin)
        }
        SparseProfile.loadFromPath(profileConfigFile).toSet.flatMap { profile: SparseProfile =>
          profile.scopes ++ loadInner(profile.subProfiles, seenSoFar + profileConfigName)
        }
      }

    loadInner(profiles, Set.empty)
  }
}

object SparseProfile {
  val customProfileName = "local"
  val profilesKey = "profiles"
  val scopesKey = "scopes"

  def custom(scopes: Set[String], subProfiles: Set[String]): SparseProfile =
    SparseProfile(customProfileName, scopes, subProfiles)

  def create(name: String)(implicit ws: StratoWorkspaceCommon): Unit =
    ws.directoryStructure.sparseProfile(name).file.create()

  def delete(name: String)(implicit ws: StratoWorkspaceCommon): Unit = {
    val profilePath = ws.directoryStructure.sparseProfile(name)
    profilePath.deleteIfExists()
  }

  def loadFromPath(path: Path)(implicit ws: StratoWorkspaceCommon): Option[SparseProfile] = {
    if (!path.exists()) None
    else {
      val config = ConfigUtils.loadConfig(path)
      def readProperty(key: String) = ConfigUtils.readProperty[Option[Set[String]]](config)(key).getOrElse(Set.empty)
      val scopes = readProperty(scopesKey)
      val profiles = readProperty(profilesKey)
      Some(SparseProfile(path.name.stripSuffix(".conf"), scopes, profiles))
    }
  }

  def load(name: String)(implicit ws: StratoWorkspaceCommon): Option[SparseProfile] = {
    val path = if (name == customProfileName) {
      ws.directoryStructure.localSparseProfileConfFile
    } else ws.directoryStructure.sparseProfile(name)
    SparseProfile.loadFromPath(path)
  }

  def loadAll()(implicit ws: StratoWorkspaceCommon): Seq[SparseProfile] = {
    val localProfile = ws.directoryStructure.localSparseProfileConfFile
    val profiles = ws.directoryStructure.allSparseProfiles
    val profilesWithLocal = if (localProfile.exists()) profiles :+ localProfile else profiles

    profilesWithLocal.flatMap(SparseProfile.loadFromPath(_).toSeq).sortBy(_.name)
  }
}
