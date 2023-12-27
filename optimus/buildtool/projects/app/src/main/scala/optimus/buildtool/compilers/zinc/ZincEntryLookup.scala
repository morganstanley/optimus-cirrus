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
package optimus.buildtool.compilers.zinc

import java.nio.file.Path
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap
import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import com.github.benmanes.caffeine.cache.RemovalCause
import optimus.buildtool.app.BuildInstrumentation
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.InternalClassFileArtifactType
import optimus.buildtool.artifacts.MessageArtifactType
import optimus.buildtool.compilers.zinc.mappers.ZincExternalReadMapper
import optimus.buildtool.compilers.zinc.mappers.ZincExternalWriteMapper
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.utils.Utils
import sbt.internal.inc._
import xsbti.VirtualFile
import xsbti.compile.CompileAnalysis
import xsbti.compile.DefinesClass
import xsbti.compile.PerClasspathEntryLookup
import xsbti.compile.analysis.ReadWriteMappers

import scala.compat.java8.OptionConverters._
import scala.util.control.NonFatal

final case class ZincEntryLookup(upstreamAnalyses: Map[VirtualFile, Path], cache: ZincAnalysisCache)
    extends PerClasspathEntryLookup {
  override def definesClass(classpathEntry: VirtualFile): DefinesClass = cache.definesClass(classpathEntry)
  override def analysis(classpathEntry: VirtualFile): Optional[CompileAnalysis] =
    upstreamAnalyses.get(classpathEntry).flatMap(cache.get).asJava
}

final case class PreviousJars(
    classes: Either[Path, JarAsset],
    messages: Either[Path, FileAsset],
    signature: Either[Path, JarAsset],
    signatureAnalysis: Either[Path, JarAsset]
) {
  def missing(requireSignatures: Boolean): Seq[Path] = {
    val requiredArtifacts =
      if (requireSignatures) Seq(classes, messages, signature, signatureAnalysis)
      else Seq(classes, messages)
    requiredArtifacts.flatMap(_.left.toOption)
  }
}

class ZincAnalysisCache(zincAnalysisCacheSize: Int, generalLogger: ZincLogger, instrumentation: BuildInstrumentation) {
  private val mappers = new ReadWriteMappers(ZincExternalReadMapper, ZincExternalWriteMapper)

  // Attempt to extract the runtime and signature jars corresponding to the analysis file
  def previousJars(
      buildDir: Directory,
      classType: InternalClassFileArtifactType,
      messageType: MessageArtifactType,
      analysisFile: JarAsset
  ): PreviousJars = {
    def asset(tpe: ArtifactType): Either[Path, JarAsset] = {
      val s = Utils.outputPathForType(analysisFile, tpe).asJar
      if (s.existsUnsafe) Right(s) else Left(s.path)
    }
    val prevClassJar = asset(classType)
    val prevMessages = asset(messageType)
    val prevSignatureJar = asset(ArtifactType.JavaAndScalaSignatures)
    val prevSignatureAnalysisJar = asset(ArtifactType.SignatureAnalysis)

    PreviousJars(prevClassJar, prevMessages, prevSignatureJar, prevSignatureAnalysisJar)
  }

  private val definesClassCache = new UniqueCache[VirtualFile, DefinesClass](
    zincAnalysisCacheSize,
    f => Some(Locate.definesClass(f)),
    generalLogger.debug(_))

  def definesClass(classpathEntry: VirtualFile): DefinesClass = definesClassCache.get(classpathEntry).get

  private val loadCount = new ConcurrentHashMap[Path, Integer]()
  private val analysisCache = new UniqueCache[Path, CompileAnalysis](
    zincAnalysisCacheSize,
    { p =>
      try {
        val t0 = System.nanoTime()
        val content = FileAnalysisStore.binary(p.toFile, mappers).get
        if (content.isPresent) {
          val t = (System.nanoTime() - t0) / 1000000L
          val n = loadCount.compute(p, (_, i) => if (i eq null) 1 else i + 1)
          generalLogger.debug(s"Analysis cache loaded (n=$n t=$t) $p")
          Some(content.get.getAnalysis)
        } else {
          generalLogger.warn(s"Cannot load $p")
          None
        }
      } catch {
        case NonFatal(e) =>
          generalLogger.warn(s"Exception loading $p: $e")
          e.printStackTrace()
          None
      }
    },
    generalLogger.debug(_)
  )

  def get(analysisPath: Path): Option[CompileAnalysis] = {
    instrumentation.signatureAnalysisRequested.foreach(_(analysisPath))
    analysisCache.get(analysisPath)
  }

  /* TODO (OPTIMUS-26649): enable direct store when we know how to map in-memory
  def addSignatureAnalysis(path: Path, a: CompileAnalysis) = {
    NamingConventions.dissectPath(buildDir, path) match {
      case Right(Dissection(_, _, _, name, hash, None)) =>
        val path = NamingConventions.reconstructPath(buildDir, NamingConventions.SIGNATURE_ANALYSIS, None, name, hash, None)
        analysisCache.put(path, a)
      case _ =>
    }
  }
   */
}

class UniqueCache[K <: AnyRef, V <: AnyRef](maxSize: Long, f: K => Option[V], debug: String => Unit) {

  private def cacheStatus: String = s"n_lru=${lru.estimatedSize()} n_parked=${weakCache.estimatedSize()}"

  private val ac = s"Analysis cache (${System.identityHashCode(this).toHexString}):"

  private val weakCache: Cache[K, Option[V]] = Caffeine
    .newBuilder()
    .weakValues()
    .removalListener { (key: K, _: Option[V], cause: RemovalCause) =>
      debug(s"$ac evicted ($cause) from weak map $key [$cacheStatus]")
    }
    .build[K, Option[V]]

  private val lru: LoadingCache[K, Option[V]] = Caffeine
    .newBuilder()
    .maximumSize(maxSize)
    .build { key =>
      Option(weakCache.getIfPresent(key)) match {
        case Some(t) =>
          // Was previously evicted from LRU, but is still in weak cache
          debug(s"$ac retrieving parked $key [$cacheStatus]")
          t
        case None =>
          // We don't have anything, so we need to compute for real.
          // We also add it to the weak cache so we can always get hold of it while
          // it's still in use.
          val t = f(key)
          debug(s"$ac computed $key [$cacheStatus]")
          weakCache.put(key, t)
          t
      }
    }

  def get(path: K): Option[V] = lru.get(path)
}
