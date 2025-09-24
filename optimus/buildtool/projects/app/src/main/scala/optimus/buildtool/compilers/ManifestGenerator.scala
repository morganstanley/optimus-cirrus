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
package optimus.buildtool.compilers

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.{ArtifactType => AT}
import optimus.buildtool.artifacts.ClassFileArtifact
import optimus.buildtool.artifacts.ElectronArtifact
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.InternalClassFileArtifact
import optimus.buildtool.artifacts.PathingArtifact
import optimus.buildtool.compilers.AsyncCppCompiler.BuildType
import optimus.buildtool.compilers.cpp.CppLibrary
import optimus.buildtool.compilers.cpp.CppUtils
import optimus.buildtool.config.ScopeConfiguration
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.FileAsset
import optimus.buildtool.resolvers.DependencyCopier
import optimus.buildtool.scope.ScopeDependencies
import optimus.buildtool.utils.JarUtils
import optimus.buildtool.utils.Jars
import optimus.buildtool.utils.OsUtils
import optimus.platform._

import java.util.jar

@entity class ManifestGenerator(dependencyCopier: DependencyCopier, cppFallback: Boolean) {

  @node def manifest(
      id: ScopeId,
      scopeConfig: ScopeConfiguration,
      internalRuntimeArtifacts: Seq[Artifact],
      internalAgentArtifacts: Seq[Artifact],
      runtimeDependencies: ScopeDependencies
  ): jar.Manifest = {

    val internalClassFileArtifacts = internalRuntimeArtifacts.collect { case c: ClassFileArtifact => c }
    val externalClassFileArtifacts =
      runtimeDependencies.transitiveExternalDependencies.apar.map { a =>
        dependencyCopier.atomicallyDepCopyExternalClassFileArtifactsIfMissing(a)
      }

    val classFileArtifacts = internalClassFileArtifacts ++ externalClassFileArtifacts

    val scopeClassFileArtifacts = internalClassFileArtifacts.collect {
      case c @ InternalClassFileArtifact(InternalArtifactId(scopeId, _, _), _) if scopeId == id =>
        c
    }

    val extraFiles = runtimeDependencies.transitiveExtraFiles

    val externalJniPaths = runtimeDependencies.transitiveJniPaths.distinct

    val cppLibs: Seq[CppLibrary] =
      CppUtils.libraries(internalRuntimeArtifacts, runtimeDependencies.transitiveScopeDependencies)
    val preloadLibs = cppLibs.filter(_.preload).groupBy(_.buildType)
    val preloadReleaseLibs = preloadLibs.getOrElse(BuildType.Release, Nil)
    val preloadDebugLibs = preloadLibs.getOrElse(BuildType.Debug, Nil)

    // Native artifact paths are used in several different places in the jar file:
    // - JniScopes: Contains the scopes with native artifacts. This is used by OAR/OTR when running apps/tests
    //     (if fallback is false)
    // - PackagedJniLibs: Contains build_obt paths to native libs within jars. This is used by IntelliJ
    //     when running apps (if fallback is false)
    // - JniFallbackPaths: Contains paths to disted native artifacts. This is used by OAR/OTR/IntellliJ when
    //     running apps/tests (if fallback is true)
    // - Preload[Type]Scopes: Contains the scopes with native artifacts for the build type (Release or Debug). This is used
    //     by OAR/OTR when running apps/tests (if fallback is false)
    // - PackagedPreload[Type]Libs: Contains build_obt paths to native libs within jars for the build type (Release or Debug).
    //     This is used by IntelliJ when running apps (if fallback is false)
    // - NativePreload[Type]FallbackPath: Contains paths to disted native artifacts for the build type (Release or Debug).
    //     This is used by OAR/OTR/IntellliJ when running apps/tests (if fallback is true, or we're running a
    //     windows app/test on the grid)

    // Include both release and debug scopes (in general these will be the same) - the app/test
    // will decide at runtime which lib to load with a `System.loadLibrary` call
    val jniScopes = cppLibs.map(_.scopeId).distinct
    // Include both release and debug version of packaged native libs - the app/test will decide at runtime
    // which to load with a `System.loadLibrary` call. Note that these are OS-specific, but are only used locally
    // by intellij
    val packagedJniLibs = cppLibs.filter(_.osVersion == OsUtils.osVersion).flatMap(_.localFile)

    // Include both release and debug fallback paths (in general these will be the same) - the app/test
    // will decide at runtime which lib to load with a `System.loadLibrary` call
    val jniFallbackPaths = cppLibs.flatMap(_.fallbackPath).distinct

    // Include both release and debug scopes - OAR/OTR will select the appropriate one at runtime
    val preloadReleaseScopes = preloadReleaseLibs.map(_.scopeId).distinct
    val preloadDebugScopes = preloadDebugLibs.map(_.scopeId).distinct

    // Include both release and debug version of packaged preload libs - IntelliJ will select the
    // appropriate one at runtime
    val packagedPreloadReleaseLibs = preloadReleaseLibs.filter(_.osVersion == OsUtils.osVersion).flatMap(_.localFile)
    val packagedPreloadDebugLibs = preloadDebugLibs.filter(_.osVersion == OsUtils.osVersion).flatMap(_.localFile)

    // Include both release and debug version of preload fallback paths - OAR/OTR/IntelliJ will select the appropriate
    // one at runtime
    def fallbackPaths(libs: Seq[CppLibrary]): Seq[FileAsset] = (for {
      lib <- libs
      fallbackPath <- lib.fallbackPath
    } yield fallbackPath.resolveFile(CppUtils.linuxNativeLibrary(lib.scopeId, lib.buildType))).distinct

    val preloadReleaseFallbackPaths = fallbackPaths(preloadReleaseLibs)
    val preloadDebugFallbackPaths = fallbackPaths(preloadDebugLibs)

    val moduleLoads = runtimeDependencies.resolution.map(_.result.moduleLoads).getOrElse(Nil)

    val (premainOption, filteredArtifacts) = scopeConfig.agentConfig
      .map { a =>
        (
          Some(a.agentClass),
          classFileArtifacts.flatMap {
            case ia: InternalClassFileArtifact if a.excluded.contains(ia.id.scopeId) && ia.id.tpe == AT.Scala =>
              None
            case artifact =>
              Some(artifact)
          }
        )
      }
      .getOrElse(None, classFileArtifacts)

    val electronMetadata = internalRuntimeArtifacts
      .collect { case e: ElectronArtifact =>
        s"${e.scopeId};${e.pathString};${e.mode};${e.executables.mkString(",")}"
      }
      .mkString(" ")

    val internalAgentPathingArtifacts = internalAgentArtifacts.collect { case p: PathingArtifact => p }
    val externalAgentClassFileArtifacts =
      runtimeDependencies.transitiveExternalDependencies.apar.collect {
        case a if a.containsAgent => dependencyCopier.atomicallyDepCopyExternalClassFileArtifactsIfMissing(a)
      }
    val allAgentPaths = internalAgentPathingArtifacts ++ externalAgentClassFileArtifacts

    Jars.updateManifest(
      Jars.createPathingManifest(filteredArtifacts.map(_.path), premainOption),
      JarUtils.nme.ClassJar -> scopeClassFileArtifacts.map(_.pathString).mkString(";"),
      JarUtils.nme.ExtraFiles -> extraFiles.map(_.pathString).mkString(";"),
      JarUtils.nme.ExternalJniPath -> externalJniPaths.mkString(";"),
      JarUtils.nme.JniScopes -> jniScopes.map(_.properPath).mkString(";"),
      JarUtils.nme.AgentsPath -> allAgentPaths.map(_.pathString).mkString(";"),
      JarUtils.nme.PackagedJniLibs -> packagedJniLibs.map(_.pathString).mkString(";"),
      JarUtils.nme.JniFallbackPath -> jniFallbackPaths.map(_.pathString).mkString(";"),
      JarUtils.nme.PreloadReleaseScopes -> preloadReleaseScopes.map(_.properPath).mkString(";"),
      JarUtils.nme.PreloadDebugScopes -> preloadDebugScopes.map(_.properPath).mkString(";"),
      JarUtils.nme.PackagedElectron -> electronMetadata,
      JarUtils.nme.PackagedPreloadReleaseLibs -> packagedPreloadReleaseLibs.map(_.pathString).mkString(";"),
      JarUtils.nme.PackagedPreloadDebugLibs -> packagedPreloadDebugLibs.map(_.pathString).mkString(";"),
      JarUtils.nme.PreloadReleaseFallbackPath -> preloadReleaseFallbackPaths.map(_.pathString).mkString(";"),
      JarUtils.nme.PreloadDebugFallbackPath -> preloadDebugFallbackPaths.map(_.pathString).mkString(";"),
      JarUtils.nme.CppFallback -> cppFallback.toString,
      JarUtils.nme.ModuleLoads -> moduleLoads.mkString(";"),
      // backward compatibility
      JarUtils.nme.JNIPath -> (jniFallbackPaths.map(_.pathString) ++ externalJniPaths).mkString(";"),
      JarUtils.nme.PreloadPath -> preloadReleaseFallbackPaths.map(_.pathString).mkString(";")
    )
  }
}

object ManifestGenerator {
  import optimus.buildtool.cache.NodeCaching.reallyBigCache
  // This is the node in which manifest generation happens. It's important that we don't lose these from
  // cache (while they are still running at least) because that can result in regeneration of the same scope
  // due to a race between checking if the output artifacts are on disk and actually writing them there after
  // generation completes.
  manifest.setCustomCache(reallyBigCache)
}
