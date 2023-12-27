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

import optimus.buildtool.builders.postbuilders.installer.BundleFingerprintsCache
import optimus.buildtool.builders.postbuilders.installer.InstallableArtifacts
import optimus.buildtool.builders.postbuilders.installer.ScopeArtifacts
import optimus.buildtool.builders.postbuilders.installer.Installer
import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.ScopeConfiguration
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.trace.InstallPomFile
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.Hashing
import optimus.platform._

import scala.collection.immutable.Seq

class MavenPomInstaller(
    scopeConfigSource: ScopeConfigurationSource,
    cache: BundleFingerprintsCache,
    pathBuilder: InstallPathBuilder,
    installVersion: String,
    mavenInstallScope: String
) extends ComponentInstaller {

  override val descriptor = "maven pom files"

  @async override def install(installable: InstallableArtifacts): Seq[FileAsset] = installPomFiles(
    installable.includedScopeArtifacts.filter(_.classJars.nonEmpty))

  @async private def installPomFiles(includedScopeArtifacts: Seq[ScopeArtifacts]): Seq[FileAsset] =
    includedScopeArtifacts.filter(_.scopeId.tpe == mavenInstallScope).apar.flatMap { i =>
      val scopeId = i.scopeId
      val pomFile = this.pomFile(scopeId, pathBuilder.mavenDir(scopeId))
      val scopeConfig = scopeConfigSource.scopeConfiguration(scopeId)
      val pomDescriptor = this.pomDescriptor(scopeId, installVersion, scopeConfig)
      val newHash = Hashing.hashStrings(pomDescriptor)
      cache.bundleFingerprints(scopeId).writeIfChanged(pomFile, newHash) {
        ObtTrace.traceTask(scopeId, InstallPomFile) {
          // maven .pom file
          Installer.writeFile(pomFile, pomDescriptor)
        }
      }
    }

  private[component] def pomFile(scopeId: ScopeId, root: Directory): FileAsset =
    root.resolveFile(s"${scopeId.module}-$installVersion.pom")

  def pomDescriptor(
      scopeId: ScopeId,
      installVersion: String,
      scopeConfiguration: ScopeConfiguration
  ): Seq[String] = {
    val prefix = "com.ms."
    val pomHead =
      s"""<?xml version="1.0" encoding="UTF-8"?>
         |<project xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd"
xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
         |  <modelVersion>4.0.0</modelVersion>""".stripMargin
    val pomTail = "</project>"

    // Basic parameters
    val groupId = s"\n  <groupId>$prefix${scopeId.metaBundle}</groupId>\n"
    val artifactId = s"  <artifactId>${scopeId.module}</artifactId>\n"
    val version = s"  <version>$installVersion</version>\n"

    // Deps
    val internalCompileDeps = scopeConfiguration.internalCompileDependencies
    val internalRuntimeDeps =
      scopeConfiguration.internalRuntimeDependencies.filter(dep => !internalCompileDeps.contains(dep))

    val externalCompileDeps = scopeConfiguration.externalCompileDependencies
    val externalRuntimeDeps =
      scopeConfiguration.externalRuntimeDependencies.filter(x => !externalCompileDeps.contains(x))

    def internalString(deps: Seq[ScopeId], scope: String): String = {
      if (deps.nonEmpty) {
        deps.map { dep =>
          if (dep.metaBundle == scopeId.metaBundle && dep.module == scopeId.module) ""
          else
            s"""    <dependency>
               |      <groupId>$prefix${dep.metaBundle}</groupId>
               |      <artifactId>${dep.module}</artifactId>
               |      <version>$installVersion</version>
               |      <scope>$scope</scope>
               |    </dependency>
               |""".stripMargin
        }.mkString
      } else ""
    }

    def externalString(deps: Seq[DependencyDefinition], scope: String): String = {
      if (deps.nonEmpty) {
        deps.map { dep =>
          val exclusions =
            if (dep.excludes.isEmpty) ""
            else {
              "\n      <exclusions>" + dep.excludes.map { exclude =>
                if (exclude.group.isDefined && exclude.name.isDefined)
                  s"""|
                      |        <exclusion>
                      |          <groupId>${exclude.group.getOrElse("")}</groupId>
                      |          <artifactId>${exclude.name.getOrElse("")}</artifactId>
                      |        </exclusion>""".stripMargin
                else ""
              }.mkString + "\n      </exclusions>"
            }

          s"""    <dependency>
             |      <groupId>${dep.group}</groupId>
             |      <artifactId>${dep.name}</artifactId>
             |      <version>${dep.version}</version>
             |      <scope>$scope</scope>$exclusions
             |    </dependency>
             |""".stripMargin
        }.mkString
      } else ""
    }

    val depsString = "  <dependencies>\n" +
      internalString(internalCompileDeps, "compile") + internalString(internalRuntimeDeps, "runtime") +
      externalString(externalCompileDeps, "compile") + externalString(externalRuntimeDeps, "runtime") +
      "  </dependencies>\n"

    // output:
    (pomHead + groupId + artifactId + version + depsString + pomTail).stripMargin
      .split("\r?\n")
      .toIndexedSeq
      .filter(_.nonEmpty)
  }
}
