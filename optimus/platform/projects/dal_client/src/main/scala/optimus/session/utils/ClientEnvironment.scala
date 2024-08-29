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
package optimus.session.utils

import java.io.File
import java.net.URLClassLoader
import java.nio.charset.Charset
import java.security.MessageDigest
import java.time.Instant
import java.util.{Map => JavaMap, WeakHashMap => JavaWeakHashMap}

import msjava.slf4jutils.scalalog.getLogger
import optimus.dsi.session.JarPathT.ExistingNonStorableJarPath
import optimus.dsi.session._
import optimus.graph.DiagnosticSettings
import optimus.platform.dal.ClientMachineIdentifier
import optimus.platform.dal.RuntimeProperties.DsiSessionClasspathTypeEmergency
import optimus.platform.dal.RuntimeProperties.DsiSessionClasspathTypeProperty
import optimus.platform.dal.SessionFetcher
import optimus.platform.dal.TreadmillNode
import optimus.platform.dal.UnremarkableClientMachine
import optimus.platform.metadatas.internal.MetaDataFiles
import optimus.platform.util.ExportedInfoScanner
import optimus.platform.ImmutableArray
import optimus.platform.dal.config.TreadmillApp
import optimus.platform.dal.config.TreadmillInstanceId
import optimus.platform.dsi.versioning.PathHashRepr
import optimus.platform.dsi.versioning.PathHashT

import scala.util.control.NonFatal

trait ClientEnvironmentT {
  def getClientMachineIdentifier: ClientMachineIdentifier
  def getTreadmillNode: Option[TreadmillNode]
  def getTreadmillApp: Option[TreadmillApp]
  def getTreadmillInstanceId: Option[TreadmillInstanceId]
}

object ClientEnvironment extends ClientEnvironmentT {
  private val log = getLogger(this)

  private val logClasspath = DiagnosticSettings.getBoolProperty("optimus.session.client.logClasspath", false)

  private val entityResolverWiseSessionEstablishmentTime: JavaMap[SessionFetcher, Instant] =
    new JavaWeakHashMap[SessionFetcher, Instant]()

  private[optimus] def initGlobalSessionEstablishmentTime(resolver: SessionFetcher, time: Instant): Option[Instant] =
    synchronized {
      Option(entityResolverWiseSessionEstablishmentTime.putIfAbsent(resolver, time))
    }

  private[optimus] def forceGlobalSessionEstablishmentTimeTo(resolver: SessionFetcher, time: Instant): Option[Instant] =
    synchronized {
      Some(entityResolverWiseSessionEstablishmentTime.put(resolver, time))
    }

  private[optimus] def getGlobalSessionEstablishmentTime(resolver: SessionFetcher): Option[Instant] = synchronized {
    Option(entityResolverWiseSessionEstablishmentTime.get(resolver))
  }

  // Besides Treadmill, there might be other interesting types of client
  // machines in the future. Be sure to add them here.
  override def getClientMachineIdentifier: ClientMachineIdentifier = {
    getTreadmillNode.getOrElse(UnremarkableClientMachine)
  }

  override def getTreadmillNode: Option[TreadmillNode] = {
    if (getTreadmillApp.isDefined && getTreadmillInstanceId.isDefined)
      Some(TreadmillNode(getTreadmillApp.get, getTreadmillInstanceId.get))
    else
      None
  }

  override def getTreadmillApp: Option[TreadmillApp] = {
    val treadmillApp = sys.env.getOrElse("TREADMILL_APP", "")
    if (treadmillApp.isEmpty)
      None
    else
      Some(TreadmillApp(treadmillApp))
  }

  override def getTreadmillInstanceId: Option[TreadmillInstanceId] = {
    val treadmillInstanceID = sys.env.getOrElse("TREADMILL_INSTANCEID", "")
    if (treadmillInstanceID.isEmpty)
      None
    else
      Some(TreadmillInstanceId(treadmillInstanceID))
  }

  private[optimus] lazy val classpath: Seq[ExistingNonStorableJarPath] = SessionUtils.classpath()

  lazy val classpathHash: PathHashT = {
    val cp = classpathType match {
      case ClasspathType.Default   => classpath
      case ClasspathType.Emergency => Seq(MissingJarPath("PANIC!"))
    }
    val hash = PathHashRepr(Hasher.toByteArray(cp))
    if (logClasspath) {
      log.info(s"classpath hash: $hash")
      log.info(s"classpath existing jars:\n${cp.map(_.path).mkString("\n")}")
    }
    hash
  }

  sealed abstract class ClasspathType
  object ClasspathType {
    case object Default extends ClasspathType
    case object Emergency extends ClasspathType
    def apply(property: String): ClasspathType =
      if (property == DsiSessionClasspathTypeEmergency) Emergency else Default
  }

  val classpathType =
    Option(System.getProperty(DsiSessionClasspathTypeProperty)) map ClasspathType.apply getOrElse ClasspathType.Default

  object Hasher {
    private[this] val digestTL = new ThreadLocal[MessageDigest] {
      override protected def initialValue = MessageDigest.getInstance("SHA-1")
    }

    private def hash(bytes: Array[Byte]): Array[Byte] = {
      digestTL.get.digest(bytes)
    }

    def toByteArray(paths: Seq[JarPathT]): ImmutableArray[Byte] = {
      pathsToByteArray(paths.map(_.path))
    }

    def pathsToByteArray(paths: Seq[String]): ImmutableArray[Byte] = {
      val implodedPaths = paths.mkString("\u0000")
      val bytes = implodedPaths.getBytes(Charset.forName("UTF-8"))
      val hashed = hash(bytes)
      ImmutableArray.wrapped(hashed)
    }
  }

  def slotMapFromPaths(paths: Seq[String]): SlotMap = {
    val reversePaths = paths.reverse
    val slotss = reversePaths.map { jarPath =>
      val file = new File(jarPath)

      try {
        val sm = new ExportedInfoScanner(
          new URLClassLoader((file.toURI.toURL :: Nil).toArray, null),
          MetaDataFiles.entityMetaDataFileName)
        val slots = sm.discoverAllInfo
        slots collect { case (className, info) if info.slotNumber != 0 => className -> info.slotNumber }
      } catch {
        case NonFatal(ex) =>
          log.warn(s"${ex.getClass.getName} while processing classpath entry ${file}: ${ex.getMessage}")
          log.debug(ex.getStackTrace.mkString("\n"))
          Map[String, Int]()
      }
    }
    SlotMap(slotss.flatten.toMap)
  }

  def slotMap(classpath: Seq[JarPathT]): SlotMap = {
    val paths = classpath.flatMap(_.pathIfExists)
    slotMapFromPaths(paths)
  }

  lazy val classpathSlotMap = slotMap(classpath)

  lazy val clientPath = {
    // NB: All stratosphere generated run scripts have the APP_DIR and APP_NAME set.
    // Repl launchers already have APP_DIR and APP_NAME set.
    // For an IDE application, it may not be there hence we use a default string.
    // For a grid application, the assumption is that engine's APP_DIR will be same as that of client's. Although it's not exactly true for
    // an application running using grid launcher from intellij but there also the engine APP_DIR is provided by the user.
    // For all other ways in which a DAL client application could connect to the broker, we resort to using the default string. That
    // means, for such cases we have to use the host:port/proid/pid/appId information to find out the client path.
    // But for majority of the cases this should be fine.
    val appDir = Option(System.getenv("APP_DIR")).getOrElse("LOCAL_WORKSPACE").replace("\\.$", "")
    val appName = Option(System.getenv("APP_NAME")).getOrElse("LOCAL_APP")
    val separator = System.getProperty("file.separator", "/")
    s"${appDir}${separator}${appName}"
  }
}
