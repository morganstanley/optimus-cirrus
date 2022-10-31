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
package optimus.platform.utils

import java.io.FileInputStream
import java.net.MalformedURLException
import java.net.URL
import java.nio.file.InvalidPathException
import java.nio.file.Path
import java.nio.file.Paths
import java.util.jar.Attributes
import java.util.jar.JarInputStream

import msjava.slf4jutils.scalalog.getLogger

import scala.collection.mutable
import scala.util.control.NonFatal

object JarLocations {
  private val log = getLogger(this)

  class JarLocation private (path: Path) {
    lazy val canonicalPath: String =
      try {
        val file = path.toFile
        val filePath = if (file.exists()) handleRealPath(path) else path
        val normalizedFilePath = filePath.normalize()
        normalizedFilePath.toString.replace("\\", "/")
      } catch {
        case NonFatal(e) =>
          log.error(s"Failure in $path", e)
          throw e
      }

    private def handleRealPath(path: Path) = {
      val realPath = path.toRealPath()
      if (realPath.toString.dropWhile(_ == '\\').contains("\\\\")) path else realPath
    }

    def parentPath = path.getParent

    def childJarLocations: Seq[JarLocation] = {
      val manifest = {
        val file = path.toFile
        if (file.exists() && file.isFile) {
          val jarInputStream = new JarInputStream(new FileInputStream(file))
          try {
            val manifest = jarInputStream.getManifest
            Option(manifest)
          } finally {
            jarInputStream.close()
          }
        } else {
          None
        }
      }
      val classpathStrings = manifest flatMap { mf =>
        val mainAttributes = mf.getMainAttributes
        val classpathAttribute = mainAttributes.get(Attributes.Name.CLASS_PATH)
        Option(classpathAttribute) map { _.toString.split(" ").toSeq }
      } getOrElse Seq.empty
      classpathStrings.filterNot(_.isEmpty).map { child =>
        JarLocation(child, parentPath)
      }
    }

    override def hashCode(): Int = canonicalPath.hashCode

    override def equals(obj: Any): Boolean = obj match {
      case j: JarLocation => canonicalPath == j.canonicalPath
      case _              => false
    }

    override def toString: String = s"JarLocation($canonicalPath)"
  }

  object JarLocation {
    def apply(maybeRelativeLocation: String, asSeenFrom: Path): JarLocation = {
      val path =
        try {
          // try to parse as a URL first, if so construct the path from that
          val uri = new URL(maybeRelativeLocation).toURI
          Paths.get(uri)
        } catch {
          case m: MalformedURLException =>
            // This wasn't a URL, instead we should be aiming to resolve is as a file path. If relative, we want it to be
            // relative to the asSeenFrom path
            asSeenFrom.resolve(maybeRelativeLocation)
          case m: IllegalArgumentException if m.getMessage == "URI has an authority component" =>
            // This error occurs when runnig grid applications using Intellij grid launcher.
            // There are some differently formated URLs that throw this exception, so they need to be resolved this way
            asSeenFrom.resolve(maybeRelativeLocation)
          case i: InvalidPathException if i.getMessage.contains("Illegal character [:] in path") =>
            // This occurs because windows local absolute paths (e.g. D:\some\path) get encoded as file:////D:/some/path,
            // so the "path" component of the URL starts with ////D:/, not D:. We need to remove the ////
            val uri = new URL(maybeRelativeLocation).getPath.substring(4)
            Paths.get(uri)
        }
      apply(path)
    }

    def apply(path: Path): JarLocation = {
      require(path.isAbsolute, s"Could not resolve the given path $path to an absolute path")
      new JarLocation(path)
    }
  }
  private def expandClasspath(jarLocation: JarLocation): Seq[JarLocation] = {
    val acc = Seq.newBuilder[JarLocation]
    val visited = mutable.Set.empty[JarLocation]
    var stack = jarLocation :: Nil

    while (stack.nonEmpty) {
      val currentLocation = stack.head
      stack = stack.tail
      if (!visited.contains(currentLocation)) {
        visited += currentLocation
        acc += currentLocation
        stack = currentLocation.childJarLocations.toList ++ stack
      }
    }

    acc.result()
  }

  def referencedJarLocations(path: String): Seq[JarLocation] = {
    val parentJarLocation = JarLocation(Paths.get(path))
    expandClasspath(parentJarLocation)
  }

  def expandedCanonicalClasspath(): Seq[String] = {
    val appClasspath = ClassPathUtils.applicationClasspath.map(_.toString)
    appClasspath.flatMap(referencedJarLocations).map(_.canonicalPath).distinct.sorted
  }
}
