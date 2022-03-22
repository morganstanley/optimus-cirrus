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

import java.io.File
import java.net.URL
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.InvalidPathException
import java.nio.file.Path
import java.nio.file.Paths
import java.{util => ju}
import java.util.jar.Attributes.Name
import java.util.jar.JarFile
import java.util.regex.Pattern

import org.slf4j.LoggerFactory.getLogger

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
 * Utilities for extracting and expanding the current classpath
 */
// This class is a cousin of its similarly named counterpart in optimus.interop.interop_client
// Please consider porting any changes over there, too.
object ClassPathUtils {
  private val log = getLogger(getClass)

  private[this] def staticMethodResult[T](klasz: String, meth: String): Option[T] =
    Try {
      reflect
        .ensureAccessible(ClassLoader.getSystemClassLoader.loadClass(klasz).getDeclaredMethod(meth))
        .invoke(null)
        .asInstanceOf[T]
    }.fold(
      throwable => {
        log.error(s"Failed to invoke static method $klasz.$meth", throwable)
        None
      },
      r => Some(r)
    )
  private[this] lazy val bootProperties =
    staticMethodResult[ju.Map[String, String]]("jdk.internal.misc.VM", "getSavedProperties").map(_.asScala)

  lazy val applicationClasspath = readClasspathEntries(getClass.getClassLoader)
  lazy val applicationClasspathString = applicationClasspath.mkString(File.pathSeparator)

  lazy val expandedApplicationClasspath = expandClasspath(applicationClasspath)
  lazy val expandedApplicationClasspathString = expandedApplicationClasspath.mkString(File.pathSeparator)

  final def readClasspathEntries(classLoader: ClassLoader, excludeRegexes: collection.Seq[String] = collection.Seq.empty): collection.Seq[Path] = {
    val excludePatterns = excludeRegexes map Pattern.compile
    def shouldExclude(path: String): Boolean =
      excludePatterns exists { _.matcher(path).matches }

    def loop(classLoader: ClassLoader): Seq[Path] = classLoader match {
      case urlCl: URLClassLoader =>
        // Zeppelin launches us in a custom classloader so check if we're in one and use its URLs too
        urlCl.getURLs.toSeq.flatMap { url =>
          if (url.getProtocol.equals("file") && !shouldExclude(url.getFile)) {
            try Some(demanglePathFromUrl(url))
            catch {
              case ex: InvalidPathException =>
                log.warn(s"Skipped invalid classpath entry: $ex")
                None
            }
          } else None
        } ++ loop(classLoader.getParent)
      case _ =>
        // The Java 9 app loader is very locked down and generally a pain to look through
        // but really it just goes off the JVM classpath! so we'll just use that.
        bootProperties
          .get("java.class.path")
          .split(File.pathSeparator)
          .filterNot(_.isEmpty) // someone does: PREPEND_APPSCRIPT_CLASSPATH=${PREPEND_APPSCRIPT_CLASSPATH}:/... and it was originally unset
          .filterNot(shouldExclude)
          .map(Paths.get(_))
          .toList
    }

    loop(classLoader)
  }

  final def demanglePathFromUrl(url: URL): Path =
    demanglePathFromUrlPath(url.getPath)

  // special handling for Windows absolute paths which in the URL start with //<driveletter>: or /<driveletter>:
  final def demanglePathFromUrlPath(path: String): Path = {
    val clean =
      if (path.charAt(0) == '/' && path.charAt(1) == '/' && path.charAt(2).isLetter && path.charAt(3) == ':')
        path.substring(2)
      else if (path.charAt(0) == '/' && path.charAt(1).isLetter && path.charAt(2) == ':')
        path.substring(1)
      else path
    Paths.get(clean)
  }

  /** Recursively expands all Class-Path manifest entries */
  final def expandClasspath(classPath: collection.Seq[Path]): collection.Seq[Path] = {
    val effectiveClassPath = new ju.LinkedHashSet[Path]()

    def loadClassPath(file: Path): Unit = {
      //ignore directories
      if (Files.isRegularFile(file)) {
        val jar = new JarFile(file.toFile)
        val mf = jar.getManifest()

        if (mf != null) {
          val pending = new ArrayBuffer[Path]
          val additionalClasspath = mf.getMainAttributes.getValue(Name.CLASS_PATH)

          if (additionalClasspath != null) {
            val paths = additionalClasspath.split(" ").map(_.trim)

            for (pathStr <- paths if pathStr.trim != "\\" && pathStr.trim != "") {
              try {
                val path = {
                  if (pathStr startsWith "file:/")
                    demanglePathFromUrl(new URL(pathStr)) // handle as a URL (probably a file URL)
                  else file.getParent.resolve(pathStr) // probably relative (but resolving an absolute path works)
                }
                if (effectiveClassPath.add(path)) pending += path
              } catch {
                case e: Exception =>
                  log.error(s"ignoring referenced path $pathStr due to error", e)
              }
            }
          }
          pending.foreach(loadClassPath)
        }
      } else log.debug(s"ignoring path as it doesn't exist ($file)")
    }

    effectiveClassPath.addAll(classPath.asJava)
    classPath.foreach(loadClassPath)
    effectiveClassPath.asScala.toSeq
  }
}
