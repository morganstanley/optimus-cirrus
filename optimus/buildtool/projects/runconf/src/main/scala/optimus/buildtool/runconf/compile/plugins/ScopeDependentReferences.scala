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
package optimus.buildtool.runconf.compile.plugins

import java.nio.file.Path

import optimus.buildtool.runconf.RunConf
import optimus.buildtool.runconf.compile.ExternalCache
import optimus.buildtool.runconf.compile.Messages
import optimus.buildtool.runconf.compile.ResolvedRunConfCompilingState
import optimus.buildtool.runconf.compile.RunConfCompilingState
import optimus.buildtool.runconf.compile.RunEnv
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory
import optimus.buildtool.config.HasMetaBundle
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.ModuleId

import scala.jdk.CollectionConverters._

object ScopeDependentReferences {
  object names {
    val module = "module"
    val name = "name"
    val scopedName = "scopedName"
    val appDir = "appDir"
    val installPath = "installPath"
    val scope = "scope"
    val selfReference = "this"
  }

  private object moduleProperty {
    def module(name: String): String = path(names.module, name)
    val appDir: String = module(names.appDir)
    val installPath: String = module(names.installPath)
    val name: String = module(names.name)
  }

  private object selfProperty {
    def self(name: String): String = path(names.selfReference, name)
    val name: String = self(names.name)
    val scopedName: String = self(names.scopedName)
    val scope: String = self(names.scope)
  }

  private def path(group: String, value: String): String = group + "." + value

}

class ScopeDependentReferences(runEnv: RunEnv, externalCache: ExternalCache) {

  import ScopeDependentReferences._

  def syntheticProperties: Config = {
    val properties = Map(
      names.module -> Seq(
        names.appDir,
        names.installPath,
        names.name
      ),
      names.selfReference -> Seq(
        names.scopedName,
        names.name,
        names.scope
      )
    )

    properties.foldLeft(ConfigFactory.empty) { case (config, (group, properties)) =>
      val props = properties.map(prop => prop -> placeholder(path(group, prop))).toMap
      config.withValue(group, ConfigValueFactory.fromMap(props.asJava))
    }
  }

  def validate(conf: RunConfCompilingState): Unit = {
    conf.validateStrings(errorMsg = Messages.appDirNotForTests) { str =>
      conf.runConf.isTest && str.contains(placeholder(moduleProperty.appDir))
    }
  }

  def resolve(conf: ResolvedRunConfCompilingState, installOverride: Option[Path]): Unit = {
    val baseCacheKey = (runEnv, installOverride)

    def substitute(path: String)(replacement: => Option[String]): String => String = { str =>
      str.replace(placeholder(path), replacement.getOrElse("_none_"))
    }

    def normalizedInstallPath(mb: MetaBundle) =
      externalCache.installPath.cached(baseCacheKey, mb) {
        installOverride
          .map(p => normalize(installPath(p, mb)))
          .getOrElse(s"${runEnv.os.makeVar("DIRNAME")}/..")
      }

    def normalizedAppDir(mb: MetaBundle) =
      externalCache.appDir.cached(baseCacheKey, mb) {
        installOverride
          .map(p => normalize(installPath(p, mb).resolve("bin")))
          .getOrElse(runEnv.os.makeVar("APP_DIR"))
      }

    val substituteAll = {
      val substituteAppDir = substitute(moduleProperty.appDir)(conf.id match {
        case mb: HasMetaBundle => Some(normalizedAppDir(mb.metaBundle))
        case _                 => None
      })
      val substituteInstallPath = substitute(moduleProperty.installPath)(conf.id match {
        case mb: HasMetaBundle => Some(normalizedInstallPath(mb.metaBundle))
        case _                 => None
      })
      val substituteModuleName = substitute(moduleProperty.name)(conf.id match {
        case m: ModuleId => Some(m.module)
        case _           => Some("_none_")
      })
      val substituteName = substitute(selfProperty.name)(Some(conf.name))
      val substituteScopedName = substitute(selfProperty.scopedName)(Some(conf.scopedName.properPath))
      val substituteScope = substitute(selfProperty.scope)(Some(conf.id.properPath))

      Seq(
        substituteAppDir,
        substituteInstallPath,
        substituteModuleName,
        substituteName,
        substituteScopedName,
        substituteScope
      ).reduce(_ andThen _)
    }

    conf.transformRunConf { case runConf: RunConf =>
      externalCache.scopeDependentResolutionRunConf.cached(baseCacheKey, runConf.runConfId, runConf)(
        runConf.modifyStrings(substituteAll))
    }
    conf.transformTemplate { template =>
      externalCache.scopeDependentResolutionTemplate.cached(baseCacheKey, template.scopedName, template)(
        template.modifyStrings(substituteAll))
    }
  }

  private def normalize(path: Path): String = {
    path.toString.replace("\\", "/")
  }

  private def installPath(root: Path, mb: MetaBundle): Path = {
    root
      .resolve(mb.meta)
      .resolve(mb.bundle)
      .resolve(runEnv.version)
      .resolve("install")
      .resolve("common")
  }

  private def placeholder(path: String): String = "###" + path + "###"

}
