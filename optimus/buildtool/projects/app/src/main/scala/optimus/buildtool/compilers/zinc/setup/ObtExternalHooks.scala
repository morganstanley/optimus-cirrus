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
package optimus.buildtool.compilers.zinc.setup

import java.nio.file.Path
import java.nio.file.Paths
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap
import optimus.buildtool.artifacts.AnalysisArtifact
import optimus.buildtool.artifacts.Artifact.InternalArtifact
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.InternalClassFileArtifactType
import optimus.buildtool.compilers.SyncCompiler.PathPair
import optimus.buildtool.compilers.zinc.LookupTracker
import optimus.buildtool.compilers.zinc.ObtReadStamps
import optimus.buildtool.compilers.zinc.ObtZincFileConverter
import optimus.buildtool.compilers.zinc.VirtualSourceFile
import optimus.buildtool.compilers.zinc.ZincCompilerFactory
import optimus.buildtool.compilers.zinc.mappers.MappingTrace
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Asset
import optimus.buildtool.format.MischiefInvalidator
import optimus.buildtool.utils.PathUtils
import sbt.internal.inc.APIs
import sbt.internal.inc.Analysis
import sbt.internal.inc.ExternalLookup
import sbt.internal.inc.UnderlyingChanges
import sbt.internal.inc.ZincInvalidationProfiler
import sbt.internal.inc.caching.ClasspathCache
import xsbti.VirtualFile
import xsbti.VirtualFileRef
import xsbti.api.AnalyzedClass
import xsbti.compile
import xsbti.compile.AnalysisContents
import xsbti.compile.Changes
import xsbti.compile.ClassFileManager
import xsbti.compile.CompileAnalysis
import xsbti.compile.DefaultExternalHooks
import xsbti.compile.ExternalHooks
import xsbti.compile.FileHash

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class ObtExternalHooks(
    scopeId: ScopeId,
    settings: ZincCompilerFactory,
    fileConverter: ObtZincFileConverter,
    previousAnalysis: Option[AnalysisContents],
    analysisMappingTrace: MappingTrace,
    sources: Seq[VirtualSourceFile],
    classpath: Seq[VirtualFile],
    fingerprintHash: String,
    outputJar: PathPair,
    classType: InternalClassFileArtifactType,
    pathToInternalClassJar: Map[Path, InternalArtifact],
    classJarToAnalysis: Map[InternalArtifact, AnalysisArtifact],
    classFileManager: ClassFileManager,
    invalidationProfiler: ZincInvalidationProfiler,
    invalidateOnly: Option[MischiefInvalidator],
    lookupTracker: Option[LookupTracker]
) extends DefaultExternalHooks(
      Optional.of(
        new ObtExternalLookup(
          settings,
          fileConverter,
          previousAnalysis,
          analysisMappingTrace,
          sources,
          classpath,
          pathToInternalClassJar,
          classJarToAnalysis,
          invalidateOnly,
          lookupTracker
        )
      ),
      Optional.of(classFileManager)
    ) {

  override val getInvalidationProfiler: compile.InvalidationProfiler =
    () => invalidationProfiler.profileRun.profiler

  override val getProvenance: ExternalHooks.GetProvenance =
    (p: Path) =>
      if (p == outputJar.tempPath.path) MappingTrace.provenance(classType, scopeId.properPath, fingerprintHash)
      else
        pathToInternalClassJar
          .get(p)
          .map { case InternalArtifact(id, a) =>
            MappingTrace.provenance(id, a)
          }
          .getOrElse("")
}

class ObtExternalLookup(
    settings: ZincCompilerFactory,
    fileConverter: ObtZincFileConverter,
    previousAnalysis: Option[AnalysisContents],
    analysisMappingTrace: MappingTrace,
    sources: Seq[VirtualSourceFile],
    classpath: Seq[VirtualFile],
    pathToInternalClassJar: Map[Path, InternalArtifact],
    classJarToAnalysis: Map[InternalArtifact, AnalysisArtifact],
    invalidateOnly: Option[MischiefInvalidator],
    lookupTracker: Option[LookupTracker]
) extends ExternalLookup {

  private val currentClasspathForComparison = classpath.map(f => sanitizePath(f.id)).toSet

  private val classIdToAnalysis = classJarToAnalysis.map { case (InternalArtifact(id, _), analysis) => id -> analysis }
  private val pathStrToInternalClassJar =
    pathToInternalClassJar.map { case (p, a) => PathUtils.platformIndependentString(p) -> a }

  private object ArtifactFromClasspath {
    def unapply(file: VirtualFileRef): Option[InternalArtifact] = pathStrToInternalClassJar.get(file.id)
  }

  private def sanitizePath(pathStr: String): String = pathStr.replaceFirst("^M:/", "//ms/")

  override def changedBinaries(previousAnalysis: CompileAnalysis): Option[Set[VirtualFileRef]] =
    Some(
      previousAnalysis
        .readStamps()
        .getAllLibraryStamps
        .asScala
        .filter { case (ref, stamp) =>
          val id = ref.id
          val immutable = id == NamingConventions.DUMMY || Asset.isJdkPlatformPath(id)
          def hasChanged = {
            val missing = !currentClasspathForComparison.contains(sanitizePath(id))
            missing || ObtReadStamps.library(fileConverter.toVirtualFile(Paths.get(id))) != stamp
          }
          !immutable && hasChanged
        }
        .keySet
        .toSet)

  private val (unchanged, prevApis) =
    previousAnalysis.fold((Set.empty[String], Map.empty[String, AnalyzedClass])) { contents =>
      (
        analysisMappingTrace.unchangedProvenances.toSet,
        contents.getAnalysis.asInstanceOf[Analysis].apis.external
      )
    }

  private val unknown = Some(APIs.emptyAnalyzedClass)
  private val unavailable = None
  private val quickAPICache = new mutable.HashMap[(Option[VirtualFileRef], String), Option[AnalyzedClass]]

  override def shouldDoEarlyOutput(compileAnalysis: CompileAnalysis): Boolean = true

  override def lookupAnalyzedClass(binaryClassName: String, file: Option[VirtualFileRef]): Option[AnalyzedClass] = {

    def lookInUpstreamAnalysis(classId: InternalArtifactId): Option[AnalyzedClass] =
      for {
        path <- classIdToAnalysis.get(classId).map(_.path)
        analysis0 <- settings.analysisCache.get(path)
        analysis = analysis0 match { case a: Analysis => a }
        // This horror is cribbed from s.i.i.IncrementalCompile
        sourceClassName <- analysis.relations.productClassName.reverse(binaryClassName).headOption
        api <- analysis.apis.internal.get(sourceClassName)
      } yield api

    val result = quickAPICache.getOrElseUpdate(
      (file, binaryClassName), {
        file match {
          // We have no idea where the fqcn might be found, so start by looking in our previous analysis.
          // This case is used when Zinc is looking for initial changes by comparing APIs in the previousAnalysis
          // with the latest upstream signature analyses.
          case None =>
            prevApis.get(binaryClassName) match {
              // If an api was stored with a provenance with an unchanged hash, we can return that api from our own
              // previous analysis rather than needing to to load it from the current upstream analysis.
              case s @ Some(api) if unchanged(api.provenance) =>
                s

              // We know about this API, but its hash has been updated.  Look for the analysis of the new classpath
              // entry. If we can't find the fqcn in the new location, then zinc will have to search for it.
              case Some(api) if !api.provenance.isEmpty =>
                val scope = MappingTrace.scopeId(api.provenance)
                val tpe = MappingTrace.artifactType(api.provenance)
                lookInUpstreamAnalysis(InternalArtifactId(scope, tpe, None)).orElse(unknown)

              // Else either the hash in the provenance has changed, or we didn't see this class at all previously,
              // so delegate to the normal search logic.
              case _ =>
                unknown
            }

          // Class was found on the classpath, and we know where its analysis ought to live.  If we don't find the
          // fqcn there, it's not going to be available anywhere, so we return definitively unavailable rather
          // than unknown as we did above.
          case Some(ArtifactFromClasspath(InternalArtifact(id, a))) =>
            val api =
              if (unchanged(MappingTrace.provenance(id, a))) prevApis.get(binaryClassName)
              else lookInUpstreamAnalysis(id)
            api.orElse(unavailable)

          // Either not on the classpath at all (which will eventually be a compilation error) or
          // in some library outside of the project, so analysis is definitely unavailable
          case _ =>
            unavailable
        }
      }
    )
    // update our lookup history
    lookupTracker.map(_.update(binaryClassName, file, result))

    result
  }

  override def changedSources(previousAnalysis: CompileAnalysis): Option[Changes[VirtualFileRef]] =
    invalidateOnly.map { invalidator =>
      new UnderlyingChanges[VirtualFileRef] {
        override def added: Set[VirtualFileRef] = Set.empty
        override def removed: Set[VirtualFileRef] = Set.empty
        override def changed: Set[VirtualFileRef] =
          sources.filter(source => invalidator.invalidates(fileConverter.toPath(source).toString)).toSet
        override def unmodified: Set[VirtualFileRef] = sources.filterNot(changed).toSet
      }
    }

  override def removedProducts(previousAnalysis: CompileAnalysis): Option[Set[VirtualFileRef]] = None
  override def shouldDoIncrementalCompilation(changedClasses: Set[String], analysis: CompileAnalysis): Boolean =
    true

  private val classPathFileHashCache = new ConcurrentHashMap[VirtualFile, FileHash]

  override def hashClasspath(files: Array[VirtualFile]): Optional[Array[FileHash]] =
    Optional.of(
      files.map(classPathFileHashCache.computeIfAbsent(
        _,
        { file =>
          val path = Paths.get(file.id)
          try {
            val pathFingerprint = PathUtils.pathFingerprint(path, Some(settings.buildDir.path))
            FileHash.of(path, pathFingerprint.hashCode)
          } catch {
            // We will most likely hit this when compiling with test libraries, e.g. for zinc itself.
            case _: IllegalArgumentException =>
              ClasspathCache.hashClasspath(List(path)).head
          }
        }
      )))
}
