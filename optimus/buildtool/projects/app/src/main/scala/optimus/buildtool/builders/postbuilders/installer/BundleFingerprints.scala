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
package optimus.buildtool.builders.postbuilders.installer

import java.nio.file.Files
import java.util.concurrent.ConcurrentHashMap
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Asset
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.files.RelativePath
import optimus.buildtool.trace._
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.OptimusBuildToolAssertions
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.TypeClasses._
import optimus.buildtool.utils.Utils
import optimus.platform._
import optimus.platform.util.Log

import scala.jdk.CollectionConverters._

/**
 * Store of fingerprints for installed jars, to make sure we're only reinstalling jars that have changed. Does not
 * support multiple independent processes writing to the same install directory.
 */
sealed class BundleFingerprints(
    bundleFile: FileAsset,
    metaBundle: MetaBundle,
    verifyInstall: Boolean,
    private var persistedState: Map[String, String]
) {
  import BundleFingerprints._

  private[this] val fingerprintHashes = new ConcurrentHashMap[String, String](persistedState.asJava)

  private[builders] def fingerprintHash(name: String): Option[String] = Option(fingerprintHashes.get(name))

  private[builders] def fingerprintHash(file: FileAsset): Option[String] =
    fingerprintHash(relativize(file))

  private[builders] def fingerprintHash(container: FileAsset, containedFile: RelativePath): Option[String] =
    fingerprintHash(relativize(container, containedFile))

  private[builders] def fingerprintHashes(container: FileAsset): Map[String, String] = {
    val prefix = s"${relativize(container)}$ContainerSeparator"
    fingerprintHashes.asScala.collect {
      case (id, hash) if id.startsWith(prefix) =>
        id.stripPrefix(prefix) -> hash
    }.toMap
  }

  private[builders] def fingerprintHashes(container: Directory): Map[String, String] = {
    val prefix = s"${relativize(container)}/"
    fingerprintHashes.asScala.collect {
      case (id, hash) if id.startsWith(prefix) =>
        id -> hash
    }.toMap
  }

  private[builders] def updateFingerprintHash(name: String, fingerprintHash: String): Unit =
    fingerprintHashes.put(name, fingerprintHash)
  private[builders] def updateFingerprintHash(file: FileAsset, fingerprintHash: String): Unit =
    fingerprintHashes.put(relativize(file), fingerprintHash)
  private[builders] def updateFingerprintHash(
      container: FileAsset,
      containedFile: RelativePath,
      fingerprintHash: String
  ): Unit =
    fingerprintHashes.put(relativize(container, containedFile), fingerprintHash)

  private def relativize(dir: Directory): String = {
    val bundleDir = bundleFile.parent
    if (!bundleDir.parent.contains(dir)) // allow dir to be in the parent so we can have .exec/... paths
      throw new IllegalArgumentException(s"${dir.pathString} is not within bundle directory ${bundleDir.pathString}")
    bundleDir.relativize(dir).pathString
  }

  private def relativize(f: FileAsset): String = {
    val bundleDir = bundleFile.parent
    if (!bundleDir.parent.contains(f)) // allow f to be in the parent so we can have .exec/... paths
      throw new IllegalArgumentException(s"${f.pathString} is not within bundle directory ${bundleDir.pathString}")
    bundleDir.relativize(f).pathString
  }
  private def relativize(container: FileAsset, containedId: String): String =
    s"${relativize(container)}$ContainerSeparator$containedId"
  private def relativize(container: FileAsset, containedFile: RelativePath): String =
    relativize(container, containedFile.pathString)

  protected def hashChanged[A <: FileAsset](whither: A, newHash: String): Boolean = {
    val oldHash = fingerprintHash(whither)
    !oldHash.contains(newHash)
  }

  def writeIfChanged[A <: FileAsset](whither: A, newHash: String)(actuallyDoIt: => Unit): Option[A] =
    (hashChanged(whither, newHash) || (verifyInstall && !whither.exists)) opt {
      actuallyDoIt /* side effect but I can't use () */
      updateFingerprintHash(whither, newHash)
      whither
    }

  @async def writeIfKeyChanged[T](key: String, newHash: String)(actuallyDoIt: => Seq[T]): Seq[T] = {
    writeIfKeyChanged$NF(key, newHash)(asAsync { () =>
      actuallyDoIt
    })
  }

  protected def hashChanged(key: String, newHash: String): Boolean = {
    val oldHash = fingerprintHash(key)
    !oldHash.contains(newHash)
  }

  @async def writeIfKeyChanged$NF[T](key: String, newHash: String)(actuallyDoIt: AsyncFunction0[Seq[T]]): Seq[T] =
    if (hashChanged(key, newHash) || verifyInstall) {
      val result = actuallyDoIt.apply()
      updateFingerprintHash(key, newHash)
      result
    } else Seq.empty

  def writeIfAnyChanged(targetDir: Directory, sourceContents: Map[RelativePath, String])(
      actuallyDoIt: => Unit): Option[Directory] = {
    def missingFiles: Boolean = sourceContents.exists { case (path, _) =>
      !targetDir.resolveFile(path).exists
    }

    writeIfAnyChanged(
      asset = targetDir,
      assetHashes = fingerprintHashes(targetDir),
      contents = sourceContents,
      fingerprintUpdater = { (file, hash) =>
        updateFingerprintHash(file.pathString, hash)
      },
      fingerprintIdentifier = { id =>
        id
      },
      missingFiles = missingFiles
    )(actuallyDoIt)
  }

  def writeIfAnyChanged[A <: FileAsset](container: A, contents: Map[RelativePath, String])(
      actuallyDoIt: => Unit
  ): Option[A] = {
    def missingFiles: Boolean = !container.exists

    writeIfAnyChanged(
      asset = container,
      assetHashes = fingerprintHashes(container),
      contents = contents,
      fingerprintUpdater = { (file, hash) =>
        updateFingerprintHash(container, file, hash)
      },
      fingerprintIdentifier = { id =>
        relativize(container, id)
      },
      missingFiles = missingFiles
    )(actuallyDoIt)
  }

  private def writeIfAnyChanged[A <: Asset](
      asset: A,
      assetHashes: Map[String, String],
      contents: Map[RelativePath, String],
      fingerprintUpdater: (RelativePath, String) => Unit,
      fingerprintIdentifier: String => String,
      missingFiles: => Boolean
  )(
      actuallyDoIt: => Unit
  ): Option[A] = {
    val changedHashes = contents.filter { case (containedFile, newHash) =>
      val oldHash = assetHashes.get(containedFile.pathString)
      !oldHash.contains(newHash)
    }
    val removedHashes = assetHashes.keySet -- contents.keySet.map(_.pathString)

    ((changedHashes ++ removedHashes).nonEmpty || (verifyInstall && missingFiles)) opt {
      actuallyDoIt /* side effect but I can't use () */
      changedHashes.foreach { case (f, h) => fingerprintUpdater(f, h) }
      removedHashes.foreach { id =>
        fingerprintHashes.remove(fingerprintIdentifier(id))
      }
      asset
    }
  }

  def removeFingerprintHash(file: FileAsset): Boolean = fingerprintHashes.remove(relativize(file)) != null

  // Need to synchronize here to ensure that what's written to the file matches the new state stored in persistedState
  def save(): FileAsset = fingerprintHashes.synchronized {
    val currentState = fingerprintHashes.asScala.toMap
    if (currentState != persistedState) {
      ObtTrace.traceTask(RootScopeId, InstallSaveState(metaBundle)) {
        AssetUtils.atomicallyWrite(bundleFile, replaceIfExists = true, backupPrevious = true) { tmp =>
          val content =
            currentState
              .map { case (name, hash) => PathUtils.fingerprintElement(name, hash) }
              .toSeq
              .sorted
          Utils.writeStringsToFile(tmp, content)
        }
        persistedState = currentState
      }
    }
    bundleFile
  }
}
object BundleFingerprints extends Log {
  private val Filename = "fingerprints.txt"
  private val ContainerSeparator = "!" // this matches JarFileSystem for consistency, but doesn't need to

  def load(pathBuilder: InstallPathBuilder, metaBundle: MetaBundle, verifyInstall: Boolean): BundleFingerprints =
    ObtTrace.traceTask(RootScopeId, InstallLoadState(metaBundle)) {
      val rootDir = pathBuilder.dirForMetaBundle(metaBundle)
      val file = rootDir.resolveFile(Filename)

      // the line here is not necessary be 'filename@hashcode', could also be 'someDir/@subDir/filename@hashcode`
      def splitHashString(line: String): Option[(String, String)] = {
        val hashFlag = s"@${NamingConventions.HASH}"
        val lastHashFlagIndex = line.lastIndexOf(hashFlag)
        if (lastHashFlagIndex >= 0) {
          val (nameWithFlag, hash) = line.splitAt(lastHashFlagIndex + 1)
          Some(nameWithFlag.dropRight(1) -> hash)
        } else {
          val msg = s"Bad fingerprint file: $file line: $line"
          if (OptimusBuildToolAssertions.enabled) throw new IllegalStateException(msg)
          else log.error(msg)
          None
        }
      }

      val fingerprints = if (file.exists) {
        import scala.io._
        val source = Source.fromInputStream(Files.newInputStream(file.path))(Codec.UTF8)
        try
          source
            .getLines()
            .flatMap(splitHashString)
            .toMap
        finally source.close()
      } else Map.empty[String, String]
      new BundleFingerprints(file, metaBundle, verifyInstall, fingerprints)
    }
}

class BundleFingerprintsCache(pathBuilder: InstallPathBuilder, verifyInstall: Boolean = false) {
  protected val allFingerprints = new ConcurrentHashMap[MetaBundle, BundleFingerprints]
  def bundleFingerprints(scopeId: ScopeId): BundleFingerprints =
    bundleFingerprints(scopeId.metaBundle)
  def bundleFingerprints(mb: MetaBundle): BundleFingerprints =
    allFingerprints.computeIfAbsent(mb, _ => BundleFingerprints.load(pathBuilder, mb, verifyInstall))

  def save(): Unit = allFingerprints.values.asScala.foreach(_.save())
}

// use for docker builds, in which incremental builds are not supported
class DisabledBundleFingerprintsCache(pathBuilder: InstallPathBuilder) extends BundleFingerprintsCache(pathBuilder) {
  override def save(): Unit = () // not saving fingerprints to disk

  override def bundleFingerprints(mb: MetaBundle): BundleFingerprints =
    allFingerprints.computeIfAbsent(
      mb,
      _ => {
        new BundleFingerprints(FileAsset.NoFile, mb, verifyInstall = false, Map.empty) {
          override protected def hashChanged[A <: FileAsset](whither: A, newHash: String): Boolean = true
          override protected def hashChanged(key: String, newHash: String): Boolean = true
          override private[builders] def updateFingerprintHash(name: String, fingerprintHash: String): Unit = ()
          override private[builders] def updateFingerprintHash(file: FileAsset, fingerprintHash: String): Unit = ()
        }
      }
    )
}
