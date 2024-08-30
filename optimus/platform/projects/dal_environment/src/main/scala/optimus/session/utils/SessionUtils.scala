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

import msjava.slf4jutils.scalalog.getLogger
import optimus.dsi.session.ExistingJarPathT
import optimus.dsi.session.JarPathT
import optimus.dsi.session.JarPathT.ExistingNonStorableJarPath
import optimus.dsi.session.MissingJarPathT
import optimus.dsi.session.NonStorableJarPath
import optimus.platform.utils.ClassPathUtils
import optimus.platform.utils.JarLocations
import optimus.platform.utils.JarLocations.JarLocation

object SessionUtils {
  private val log = getLogger(this)
  val currentUser: String = System.getProperty("user.name")

  def buildClassPathsFromJarPaths(jarLocations: Seq[JarLocation]): Seq[NonStorableJarPath] = {
    jarLocations.map { jl =>
      val location = jl.canonicalPath
      JarPathT(location)
    }
  }

  def allReferencedJars(paths: Seq[String]): Seq[NonStorableJarPath] = {
    paths flatMap allReferencedJars
  }

  def allReferencedJars(path: String): Seq[NonStorableJarPath] = {
    buildClassPathsFromJarPaths(JarLocations.referencedJarLocations(path))
  }

  private def readClasspathEntries(classLoader: ClassLoader, excludedRegexes: List[String]): Seq[String] =
    ClassPathUtils.readClasspathEntries(classLoader, excludedRegexes).map(_.toString)

  def allReferencedJars(): Seq[NonStorableJarPath] = {
    val excludeRegexes = List(
      ".*msjava/(PROJ/)?oraclejdk/.*",
      ".*ossjava/(PROJ/)?scala/.*",
      ".*ossscala/(PROJ/)?scala/.*",
      ".*/\\*"
    ) // avoid java.nio.file.InvalidPathException: Illegal char <*>
    val appClasspath = readClasspathEntries(this.getClass.getClassLoader, excludeRegexes)
    log.info(s"Application classpath is: ${appClasspath.mkString(":")}")
    allReferencedJars(appClasspath)
  }

  def classpath(): Seq[ExistingNonStorableJarPath] = {
    val jarPaths = SessionUtils.allReferencedJars()
    jarPaths.flatMap {
      case m: MissingJarPathT =>
        log.debug(s"Missing path encountered while expanding current classpath: ${m.path}")
        None
      case e: ExistingJarPathT => Some(e)
    }
  }
}
