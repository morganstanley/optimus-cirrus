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
package optimus.buildtool.builders.postbuilders.installer.component

import java.nio.file.Files
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import optimus.buildtool.builders.postbuilders.installer.BundleFingerprintsCache
import optimus.buildtool.builders.postbuilders.installer.InstallableArtifacts
import optimus.buildtool.builders.postbuilders.installer.ScopeArtifacts
import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScopeConfiguration
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.trace.InstallIvyFile
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.Hashing
import optimus.platform._

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq

class IvyInstaller(
    scopeConfigSource: ScopeConfigurationSource,
    cache: BundleFingerprintsCache,
    pathBuilder: InstallPathBuilder,
    installVersion: String
) extends ComponentInstaller {

  override val descriptor = "ivy files"

  @async override def install(installable: InstallableArtifacts): Seq[FileAsset] =
    installIvyFiles(installable.includedScopeArtifacts)

  @async private def installIvyFiles(includedScopeArtifacts: Seq[ScopeArtifacts]) = {
    val timestamp = patch.MilliInstant.now
    // Don't install ivy artifacts for closed scopes
    includedScopeArtifacts.apar.flatMap { i =>
      val scopeId = i.scopeId
      val ivyFile = this.ivyFile(scopeId)

      val scopeConfig = scopeConfigSource.scopeConfiguration(scopeId)
      if (scopeConfig.open) {
        val ivyDescriptor = this.ivyDescriptor(scopeId, timestamp, i.classJars.nonEmpty, scopeConfig)
        val timestampStr = IvyInstaller.timestampStr(timestamp)
        val newHash = Hashing.hashStrings(ivyDescriptor.map(_.replace(timestampStr, "<timestamp>")))
        cache.bundleFingerprints(scopeId).writeIfChanged(ivyFile, newHash) {
          ObtTrace.traceTask(scopeId, InstallIvyFile) {
            writeIvyFile(ivyFile, ivyDescriptor)
          }
        }
      } else None
    }
  }

  private[component] def ivyFile(scopeId: ScopeId): FileAsset = {
    val ivyDir =
      pathBuilder.etcDir(scopeId).resolveDir("ivys").resolveDir(NamingConventions.scopeOutputName(scopeId, suffix = ""))
    ivyDir.resolveFile("ivy.xml")
  }

  def ivyDescriptor(
      scopeId: ScopeId,
      timestamp: Instant,
      includeArtifact: Boolean,
      scopeConfig: ScopeConfiguration
  ): Seq[String] = {
    IvyInstaller
      .ivyDescriptor(
        scopeId,
        installVersion,
        timestamp,
        includeArtifact,
        scopeConfig.flags.installSources,
        scopeConfig.internalCompileDependencies,
        scopeConfig.externalCompileDependencies,
        scopeConfig.internalRuntimeDependencies,
        scopeConfig.externalRuntimeDependencies
      )
  }

  def writeIvyFile(ivyFile: FileAsset, descriptor: Seq[String]): Unit = {
    Files.createDirectories(ivyFile.parent.path)
    AssetUtils.atomicallyWrite(ivyFile, replaceIfExists = true, localTemp = true) { tempPath =>
      Files.write(tempPath, descriptor.asJava)
    }
  }
}

object IvyInstaller {
  private val timestampFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneOffset.UTC)

  def timestampStr(timestamp: Instant): String = timestampFormatter.format(timestamp)

  private def ivyDescriptor(
      scopeId: ScopeId,
      installVersion: String,
      timestamp: Instant,
      includeArtifact: Boolean,
      installSources: Boolean,
      internalCompileDependencies: Seq[ScopeId],
      externalCompileDependencies: Seq[DependencyDefinition],
      internalRuntimeDependencies: Seq[ScopeId],
      externalRuntimeDependencies: Seq[DependencyDefinition]
  ): Seq[String] = {
    val timestampStr = this.timestampStr(timestamp)

    val artifactName = NamingConventions.scopeOutputName(scopeId, suffix = "")
    val artifact =
      if (includeArtifact)
        s"""    <artifact name="$artifactName" type="jar" ext="jar" conf="artifacts"/>"""
      else ""
    val sources =
      if (includeArtifact && installSources)
        s"""    <artifact name="$artifactName" type="src" ext="src.jar" conf="artifacts"/>"""
      else ""

    val internalDependencies = dependencies(internalCompileDependencies, internalRuntimeDependencies).map {
      case (d, configs) =>
        val name = NamingConventions.scopeOutputName(d, suffix = "")
        val conf = configs.mkString(",")
        s"""    <dependency org="${d.meta}.${d.bundle}" name="$name" rev="$installVersion" conf="$conf"/>"""
    }

    val externalDependencies = dependencies(externalCompileDependencies, externalRuntimeDependencies).map {
      case (d, configs) =>
        val versionAttr = s""" rev="${d.version}""""
        val configAttrText = configs match {
          case Seq(conf) if conf == d.configuration => conf
          case _                                    => s"${configs.mkString(",")}-&gt;${d.configuration}"
        }

        // dependencies are transitive by default
        val transitiveAttr = if (!d.transitive) s""" transitive="false"""" else ""

        if (d.ivyArtifacts.isEmpty && d.excludes.isEmpty) {
          s"""    <dependency org="${d.group}" name="${d.name}"$versionAttr conf="$configAttrText"$transitiveAttr/>"""
        } else {
          // Use "artifact" rather than "include" as a tag name here, since extra artifacts shouldn't actually be
          // "extra" ones, they should be the only ones.
          val extraArtifacts = d.ivyArtifacts.map { a =>
            s"""      <artifact name="${a.name}" type="${a.tpe}" ext="${a.ext}"/>"""
          }

          val excludes = d.excludes.map { e =>
            s"""      <exclude org="${e.group.getOrElse("*")}" name="${e.name.getOrElse("*")}"/>"""
          }

          s"""    <dependency org="${d.group}" name="${d.name}"$versionAttr conf="$configAttrText"$transitiveAttr>
             |${extraArtifacts.mkString("\n")}
             |${excludes.mkString("\n")}
             |    </dependency>
             |""".stripMargin
        }
    }

    s"""<?xml version="1.0" encoding="UTF-8"?>
       |<ivy-module version="2.0" xmlns:m="http://ant.apache.org/ivy/maven">
       |  <info organisation="${scopeId.meta}.${scopeId.bundle}" module="${scopeId.module}" revision="$installVersion" status="release" default="true" publication="$timestampStr"/>
       |  <configurations>
       |    <conf name="artifacts" visibility="private"/>
       |    <conf name="compile" visibility="public" extends="artifacts"/>
       |    <conf name="runtime" visibility="public" extends="artifacts"/>
       |  </configurations>
       |  <publications>
       |$artifact
       |$sources
       |  </publications>
       |  <dependencies>
       |${internalDependencies.mkString("\n")}
       |${externalDependencies.mkString("\n")}
       |  </dependencies>
       |</ivy-module>
       |""".stripMargin.split("\r?\n").toIndexedSeq.filter(!_.isEmpty)
  }

  private def dependencies[A](compile: Seq[A], runtime: Seq[A]): Seq[(A, Seq[String])] = {
    val configs = (compile.map(d => (d, "compile")) ++ runtime.map(d => (d, "runtime"))).toGroupedMap
    // preserve order
    (runtime ++ compile).distinct.map(d => (d, configs(d)))
  }

}
