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
package optimus.buildtool.cache
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.buildtool.artifacts.CachedArtifactType
import optimus.buildtool.artifacts.PathedArtifact
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.FileAsset
import optimus.buildtool.trace.ObtCrumbSource
import optimus.buildtool.trace.ObtStats
import optimus.buildtool.utils.Hashing
import optimus.core.ChainedNodeID
import optimus.logging.LoggingInfo
import optimus.platform._
import optimus.stratosphere.config.StratoWorkspace
import spray.json.DefaultJsonProtocol._

import java.net.URL
import java.nio.file.Files
import scala.util.Using

trait ComparableArtifactStore extends RemoteAssetStore with ArtifactStore {
  def logStatus(): Seq[String]
  def incompleteWrites: Int
}

object ComparisonFailureBreadCrumbReporter
    extends ((ComparingArtifactStore.ComparisonContext, String, StratoWorkspace) => Unit) {
  import ComparingArtifactStore._
  def apply(context: ComparisonContext, message: String, ws: StratoWorkspace): Unit = {
    val addHocContext = Map(
      "obtVersion" -> ws.obtVersion,
      "stratoVersion" -> ws.stratosphereVersion,
      "stratoWorkspace" -> ws.stratosphereWorkspace)
    val props: Properties.Elems = Properties.Elems(
      Properties.obtCategory -> "RemoteStoreComparison",
      Properties.message -> message,
      Properties.host -> LoggingInfo.getHost,
      Properties.user -> LoggingInfo.getUser,
      Properties.osName -> LoggingInfo.os
    ) + Properties.adhocElems(context.properties) + Properties.adhocElems(addHocContext)
    Breadcrumbs.info(
      ChainedNodeID.nodeID,
      PropertiesCrumb(_, ObtCrumbSource, props)
    )
  }
}

object ComparingArtifactStore {
  final case class ComparisonContext(properties: Map[String, String])
  def commonPropertiesMap(
      cacheOperation: String,
      artifactOrAsset: String,
      authoritativeCacheClass: String,
      comparisonCacheClass: String): Map[String, String] = {

    Map(
      "cacheOperation" -> cacheOperation,
      "artifactOrAsset" -> artifactOrAsset,
      "authoritativeCacheClass" -> authoritativeCacheClass,
      "comparisonCacheClass" -> comparisonCacheClass
    )

  }

  def assetPropertiesMap(url: URL): Map[String, String] = {
    Map(
      "url" -> url.toString
    )
  }

  private def assetContext(
      url: URL,
      authoritativeCacheClass: String,
      comparisonCacheClass: String,
      operation: String): ComparisonContext = {
    ComparisonContext(
      commonPropertiesMap(operation, "asset", authoritativeCacheClass, comparisonCacheClass) ++ assetPropertiesMap(url))
  }
  private def artifactContext(
      id: ScopeId,
      fingerprintHashes: Set[String],
      tpe: String,
      discriminator: Option[String],
      authoritativeCacheClass: String,
      comparisonCacheClass: String,
      operation: String): ComparisonContext = {
    ComparisonContext(
      commonPropertiesMap(operation, "artifact", authoritativeCacheClass, comparisonCacheClass) ++ Map(
        "id" -> id.toString,
        "fingerprintHashes" -> s"[${fingerprintHashes.mkString(",")}]",
        "tpe" -> tpe,
        "discriminator" -> discriminator.toString
      ))
  }

  def FileAssetPutContext(url: URL, authoritativeCacheClass: String, comparisonCacheClass: String): ComparisonContext =
    assetContext(url, authoritativeCacheClass, comparisonCacheClass, "put")

  def FileAssetGetContext(url: URL, authoritativeCacheClass: String, comparisonCacheClass: String): ComparisonContext =
    assetContext(url, authoritativeCacheClass, comparisonCacheClass, "get")

  def FileAssetCheckContext(
      url: URL,
      authoritativeCacheClass: String,
      comparisonCacheClass: String): ComparisonContext =
    assetContext(url, authoritativeCacheClass, comparisonCacheClass, "check")

  def PathedArtifactGetContext[A <: PathedArtifact](
      id: ScopeId,
      fingerprintHash: String,
      tpe: String,
      discriminator: Option[String],
      authoritativeCacheClass: String,
      comparisonCacheClass: String): ComparisonContext =
    artifactContext(id, Set(fingerprintHash), tpe, discriminator, authoritativeCacheClass, comparisonCacheClass, "get")

  def PathedArtifactCheckContext[A <: PathedArtifact](
      id: ScopeId,
      fingerprintHashes: Set[String],
      tpe: String,
      discriminator: Option[String],
      authoritativeCacheClass: String,
      comparisonCacheClass: String): ComparisonContext =
    artifactContext(id, fingerprintHashes, tpe, discriminator, authoritativeCacheClass, comparisonCacheClass, "check")

  def PathedArtifactPutContext[A <: PathedArtifact](
      id: ScopeId,
      fingerprintHash: String,
      tpe: String,
      discriminator: Option[String],
      authoritativeCacheClass: String,
      comparisonCacheClass: String): ComparisonContext =
    artifactContext(id, Set(fingerprintHash), tpe, discriminator, authoritativeCacheClass, comparisonCacheClass, "put")

}

class ComparingArtifactStore(
    val authoritativeStore: ComparableArtifactStore,
    val otherStores: Seq[ComparableArtifactStore],
    val stratoWorkspace: StratoWorkspace,
    val onComparisonFailure: (ComparingArtifactStore.ComparisonContext, String, StratoWorkspace) => Unit =
      ComparisonFailureBreadCrumbReporter)
    extends ArtifactStoreBase
    with ComparableArtifactStore {
  import ComparingArtifactStore._
  @async private def compareAndReport(expected: Boolean, actual: Boolean)(implicit ctx: ComparisonContext): Unit = {
    if (expected != actual) {
      onComparisonFailure(ctx, s"ComparingArtifactStore: $expected != $actual", stratoWorkspace)
    }
  }

  @async private def compareAndReport(expected: Set[String], actual: Set[String])(implicit
      ctx: ComparisonContext): Unit = {
    if (!expected.equals(actual)) {
      onComparisonFailure(ctx, s"ComparingArtifactStore: $expected != $actual", stratoWorkspace)
    }
  }

  @async private def compareAndReport[A](expected: Option[A], actual: Option[A])(implicit
      ctx: ComparisonContext): Unit = {
    // if compare options
    (expected, actual) match {
      case (Some(e), Some(a)) => compareAndReport(e, a)
      case (None, None)       =>
      case _ => onComparisonFailure(ctx, s"ComparingArtifactStore: $expected != $actual", stratoWorkspace)
    }
  }

  @async private def compareAndReportAsset(expected: FileAsset, actual: FileAsset)(implicit
      ctx: ComparisonContext): Unit = {
    // read content of expected and actual and compare
    if (expected.exists && actual.exists) {
      val expectedHash = Hashing.hashFileContent(expected, true)
      val actualHash = Hashing.hashFileContent(actual, true)

      if (expectedHash != actualHash) {
        onComparisonFailure(
          ctx,
          s"ComparingArtifactStore: content of $expected ($expectedHash) and $actual ($actualHash) differ",
          stratoWorkspace)
      }

    } else if (expected.exists != actual.exists) {
      onComparisonFailure(
        ctx,
        s"ComparingArtifactStore: $expected.exists=${expected.exists} but $actual.exists=${actual.exists}",
        stratoWorkspace)
    }
  }

  @async private[buildtool] def compareAndReport[A](expected: A, actual: A)(implicit ctx: ComparisonContext): Unit = {
    (expected, actual) match {
      case (e: FileAsset, a: FileAsset) => compareAndReportAsset(e, a)
      case (e: PathedArtifact, a: PathedArtifact) =>
        compareAndReportArtifact(e, a)
    }
  }

  @async private def compareAndReportArtifact[A <: PathedArtifact](expected: A, actual: A)(implicit
      ctx: ComparisonContext): Unit = {
    val bothExist = expected.existsUnsafe && actual.existsUnsafe

    // read content of expected and actual and compare
    if (bothExist) {
      Using.Manager { use =>
        val expectedContent = use(Files.newInputStream(expected.path))
        val actualContent = use(Files.newInputStream(actual.path))
        val expectedHash = Hashing.hashBinaryStream(() => expectedContent)
        val actualHash = Hashing.hashBinaryStream(() => actualContent)

        if (expectedHash != actualHash) {
          onComparisonFailure(ctx, s"ComparingArtifactStore: content of $expected and $actual differ", stratoWorkspace)
        }
      }
    } else if (expected.exists != actual.exists) {
      onComparisonFailure(
        ctx,
        s"ComparingArtifactStore: $expected.exists=${expected.exists} but $actual.exists=${actual.exists}",
        stratoWorkspace)
    }
  }

  @async override def close(): Unit = (authoritativeStore +: otherStores).apar.foreach(_.close())

  @async override protected[buildtool] def write[A <: CachedArtifactType](
      tpe: A)(id: ScopeId, fingerprintHash: String, discriminator: Option[String], artifact: A#A): A#A = {
    val result = authoritativeStore.write(tpe)(id, fingerprintHash, discriminator, artifact)
    otherStores.apar.foreach { store =>
      val other = store.write(tpe)(id, fingerprintHash, discriminator, artifact)
      compareAndReport(result, other)(
        PathedArtifactPutContext(
          id,
          fingerprintHash,
          tpe.name,
          discriminator,
          authoritativeStore.getClass.getCanonicalName,
          store.getClass.getCanonicalName))
    }
    result
  }
  override def flush(timeoutMillis: Long): Unit = {
    authoritativeStore.flush(timeoutMillis)
    otherStores.foreach(_.flush(timeoutMillis))
  }
  @async override def get[A <: CachedArtifactType](
      id: ScopeId,
      fingerprintHash: String,
      tpe: A,
      discriminator: Option[String]): Option[A#A] = {
    val result = authoritativeStore.get(id, fingerprintHash, tpe, discriminator)
    otherStores.apar.foreach { store =>
      val other = store.get(id, fingerprintHash, tpe, discriminator)
      compareAndReport(result, other)(
        PathedArtifactGetContext[A#A](
          id,
          fingerprintHash,
          tpe.name,
          discriminator,
          authoritativeStore.getClass.getCanonicalName,
          store.getClass.getCanonicalName))
    }
    result
  }
  @async override def check[A <: CachedArtifactType](
      id: ScopeId,
      fingerprintHashes: Set[String],
      tpe: A,
      discriminator: Option[String]): Set[String] = {
    val result = authoritativeStore.check(id, fingerprintHashes, tpe, discriminator)
    otherStores.apar.foreach { store =>
      val other = store.check(id, fingerprintHashes, tpe, discriminator)
      compareAndReport(result, other)(
        PathedArtifactCheckContext(
          id,
          fingerprintHashes,
          tpe.name,
          discriminator,
          authoritativeStore.getClass.getCanonicalName,
          store.getClass.getCanonicalName))
    }
    result
  }
  override protected def cacheType: String = s"RemoteComparing"
  override protected def stat: ObtStats.Cache = ObtStats.RemoteComparing

  @async override def get(url: URL, destination: FileAsset): Option[FileAsset] = {
    implicit val result: Option[FileAsset] = authoritativeStore.get(url, destination)
    otherStores.apar.foreach { store =>
      val compFileName = destination.path.getFileName.toString + ".comp"
      val compDestination = FileAsset(destination.path.resolveSibling(compFileName))

      val other = store.get(url, compDestination)
      compareAndReport(result, other)(
        FileAssetGetContext(url, authoritativeStore.getClass.getCanonicalName, store.getClass.getCanonicalName))
    }
    result
  }
  @async override def put(url: URL, file: FileAsset): FileAsset = {
    val result = authoritativeStore.put(url, file)
    otherStores.apar.foreach { store =>
      val other = store.put(url, file)
      compareAndReport(result, other)(
        FileAssetPutContext(url, authoritativeStore.getClass.getCanonicalName, store.getClass.getCanonicalName))
    }
    result
  }
  @async override def check(url: URL): Boolean = {
    val result = authoritativeStore.check(url)
    otherStores.apar.foreach { store =>
      val other = store.check(url)
      compareAndReport(result, other)(
        FileAssetCheckContext(url, authoritativeStore.getClass.getCanonicalName, store.getClass.getCanonicalName))
    }
    result
  }
  override def logStatus(): Seq[String] = authoritativeStore.logStatus()
  override def incompleteWrites: Int = authoritativeStore.incompleteWrites
}
