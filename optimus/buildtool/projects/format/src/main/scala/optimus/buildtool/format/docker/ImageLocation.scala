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
package optimus.buildtool.format.docker

import com.google.cloud.tools.jib.api._
import com.google.common.annotations.VisibleForTesting
import java.nio.file.Path
import java.nio.file.Paths
import scala.collection.compat._
import scala.util.Properties

/**
 * For some odd reason the JIB API has a bunch of image location classes that it disambiguates with overloads.
 * (Internally they have a common representation but I'm not keen on cracking it open just for this.) Therefore, here's
 * an ADT of all of them we wish to support.
 */
sealed abstract class ImageLocation {
  def name: String
  val tag: String = ImageReference.parse(name).getTag.orElse("latest")

  import ImageLocation.MakeBuilder
  // i.e., use this as a base image
  def mkBuilder: MakeBuilder[JibContainerBuilder]
  // i.e., use this as a target image
  def mkContainerizer: MakeBuilder[Containerizer]
}

object ImageLocation {
  type MakeBuilder[T] = (T => Unit) => T
  private def tap[T](from: T): MakeBuilder[T] = (f: T => Unit) => { f(from); from }

  // This looks like code duplication but is not; the `from` and `to` methods are overloaded.
  final case class Daemon(name: String) extends ImageLocation {
    val value: DockerDaemonImage = DockerDaemonImage.named(name).setDockerExecutable(Docker)
    def mkBuilder = tap(Jib.from(value))
    def mkContainerizer = tap(Containerizer.to(value))
  }
  final case class File(path: Path, name: String) extends ImageLocation {
    val value: TarImage = TarImage.at(path).named(name)
    def mkBuilder = tap(Jib.from(value))
    def mkContainerizer = tap(Containerizer.to(value))

    override def toString(): String = s"$path@$name"
  }
  final case class Registry(name: String) extends ImageLocation {
    val value: RegistryImage = RegistryImage.named(name)
    def mkBuilder = tap(Jib.from(value))
    def mkContainerizer = tap(Containerizer.to(value))
  }

  class StartingWith(pre: String) {
    def unapply(str: String): Option[String] = if (str.startsWith(pre)) Some(str.stripPrefix(pre)) else None
  }

  final val DaemonPre = new StartingWith(Jib.DOCKER_DAEMON_IMAGE_PREFIX)
  final val RegistryPre = new StartingWith(Jib.REGISTRY_IMAGE_PREFIX)
  final val FilePre = new StartingWith(Jib.TAR_IMAGE_PREFIX)

  @VisibleForTesting private[docker] var dockerOverride: Option[Path] = None
  lazy val Docker = dockerOverride getOrElse Paths.get {
    import sys.process._
    def fail() =
      throw new AssertionError("can't proceed without 'docker' on PATH")
    try "/usr/bin/which docker".!!.linesIterator.to(Seq).headOption.getOrElse(fail())
    catch { case re: RuntimeException => fail() }
  }

  @throws[IllegalArgumentException]
  def parse(str: String, registry: Option[String] = None): ImageLocation = {
    def rethrow(img: => ImageLocation) =
      try img
      catch { case e: InvalidImageReferenceException => throw new IllegalArgumentException(e) }
    def asDaemon(name: String) =
      rethrow(Daemon(name))
    def asRegistry(name: String) = {
      val r = Registry(name)
      rethrow(r)
    }
    def asFile(arg: String) = rethrow {
      val (path, name) = arg.split("@", 2) match {
        case Array(p) =>
          val path = Paths.get(p)
          path -> path.getFileName.toString.stripSuffix(".tar")
        case Array(p, n) =>
          Paths.get(p) -> n
      }
      if (path.isAbsolute) File(path, name)
      else throw new IllegalArgumentException("TAR image reference should be absolute")
    }
    str match {
      case DaemonPre(name)   => asDaemon(name)
      case RegistryPre(name) => asRegistry(name)
      case FilePre(arg)      => asFile(arg)
      case arg               => registry.fold(asFile(arg))(r => parse(s"registry://$r/$arg"))
    }
  }

  import org.kohsuke.args4j.{CmdLineParser, OptionDef, CmdLineException, spi}
  final class Handler(parser: CmdLineParser, opt: OptionDef, setter: spi.Setter[_ >: ImageLocation])
      extends spi.OneArgumentOptionHandler[ImageLocation](parser, opt, setter) {
    override def parse(argument: String): ImageLocation = {
      complainOnWindows()
      try ImageLocation.parse(argument)
      catch {
        case iae: IllegalArgumentException =>
          iae.getCause match {
            case ul: InvalidImageReferenceException =>
              throw new CmdLineException(parser, ul)
            case _ =>
              throw new CmdLineException(parser, iae)
          }
      }
    }

    private def complainOnWindows(): Unit = {
      if (Properties.isWin)
        throw new CmdLineException(parser, "Can't build docker image on Windows (yet)", null)
    }
  }
}
