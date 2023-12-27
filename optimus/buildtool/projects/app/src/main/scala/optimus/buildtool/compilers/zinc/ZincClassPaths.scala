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

import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes

import optimus.buildtool.files.JarAsset

import scala.collection.mutable

object ZincClassPaths {

  private val zincVersionRequiredIfStartingWith =
    "compiler-bridge" :: "minimalaggregate" :: "zinc" :: "compiler-interface" :: Nil
  private val sourceRequiredFor = "compiler-bridge"
  private val versionTag = "_\\d+(\\.\\d+)+(-M\\d+)?$".r
  def skipForZincRun(name: String): Boolean = name.startsWith("compiler-interface")

  // Assemble the classpath for zinc by walking the installation directory.
  def zincClassPath(zincHome: Path, zincVersion: String, scalaVersion: String, forZincRun: Boolean): Seq[JarAsset] = {
    val jars = mutable.Buffer[JarAsset]()
    val zincVersionTag = ("-" + zincVersion + "(-|\\.)").r

    assert(versionTag.findFirstMatchIn(s"_$scalaVersion").isDefined, s"Scala version $scalaVersion not of form X.XX")
    Files.walkFileTree(
      zincHome,
      new SimpleFileVisitor[Path] {
        // Ignore directories that seem to contain versions of scala other than ours.
        override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
          val name = dir.getFileName.toString
          if (forZincRun && skipForZincRun(name))
            FileVisitResult.SKIP_SUBTREE
          else if (versionTag.findFirstMatchIn(name).isDefined && !name.endsWith(scalaVersion))
            FileVisitResult.SKIP_SUBTREE
          else
            FileVisitResult.CONTINUE
        }
        // Ignore source jars with the one exception of compiler-bridge.
        override def visitFile(path: Path, attrs: BasicFileAttributes): FileVisitResult = {
          val name = path.getFileName.toString
          val isJar = name.endsWith("jar")
          val isSource = isJar && name.endsWith("sources.jar")
          val isDoc = isJar && name.endsWith("doc.jar")
          val versionOk = !zincVersionRequiredIfStartingWith.exists(name.startsWith) ||
            zincVersionTag.findFirstMatchIn(name).isDefined
          if (versionOk && isJar && !isDoc && (!isSource ^ name.startsWith(sourceRequiredFor))) jars += JarAsset(path)
          FileVisitResult.CONTINUE
        }
      }
    )
    jars.toList // force conversion to an immutable type
  }
}
