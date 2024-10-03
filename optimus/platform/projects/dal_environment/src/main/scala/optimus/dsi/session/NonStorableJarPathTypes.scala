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
package optimus.dsi.session

import com.typesafe.config.ConfigFactory

import java.io.File
import java.util.regex.Pattern

private[optimus] trait JarPathT {
  def path: String
  def pathIfExists: Option[String]
}

private[optimus] object JarPathT {
  val config = ConfigFactory.parseResourcesAnySyntax(getClass().getClassLoader(), "properties.conf")

  val MprPathBaseDir = config.getString("paths.mpr-path-base-dir")
  private val mprPathPatternStr = config.getString("paths.mpr-path-pattern")
  private val mprPathPattern = Pattern.compile(mprPathPatternStr)
  private val depcopyPattern = ".*/.stratosphere/depcopy/dist/(.*)".r

  def apply(path: String): NonStorableJarPath = {
    def splitPath(p: String) = p.split("/").toList.dropWhile(_.isEmpty)
    val file = new File(path)
    // We expect only "file" path, not directory/folder path..
    if (file.exists() && file.isFile) {
      val matcher = mprPathPattern.matcher(path)
      if (matcher.matches()) {
        val meta = matcher.group(1)
        val project = matcher.group(2)
        val release = matcher.group(3)
        val relativePath = matcher.group(4)
        MprJarPath(meta, project, release, relativePath)
      } else {
        path match {
          case depcopyPattern(c) =>
            splitPath(c) match {
              case meta :: "PROJ" :: project :: release :: relativePath =>
                MprJarPath(meta, project, release, relativePath.mkString("/"))
              case _ => AbsoluteJarPath(path)
            }
          case _ => AbsoluteJarPath(path)
        }
      }
    } else MissingJarPath(path)
  }
  type ExistingNonStorableJarPath = ExistingJarPathT with NonStorableJarPath
}

/**
 * This type represents a jar-path to be stored in the DAL as a part of (Mpr)Classpath entity.
 */
trait StorableJarPath extends JarPathT

/**
 * This type is solely for internal representation between client-broker.
 */
trait NonStorableJarPath extends JarPathT

private[optimus] trait ExistingJarPathT extends JarPathT {
  override final def pathIfExists: Option[String] = Some(path)
}
private[optimus] trait MissingJarPathT extends JarPathT {
  final override def pathIfExists: Option[String] = None
}

private[optimus] trait MprJarPathT extends ExistingJarPathT {
  require(
    !relativePath.startsWith("./") && !relativePath.startsWith("../") && !relativePath.startsWith("/"),
    s"relativePath $relativePath should not start with './', '../', or '/'"
  )
  def meta: String
  def project: String
  def release: String
  def relativePath: String

  import optimus.dsi.session.JarPathT.MprPathBaseDir
  final override def path: String = s"$MprPathBaseDir/$meta/PROJ/$project/$release/$relativePath"
}
private[optimus] trait AbsoluteJarPathT extends ExistingJarPathT

private[optimus] final case class MprJarPath(meta: String, project: String, release: String, relativePath: String)
    extends MprJarPathT
    with NonStorableJarPath
private[optimus] final case class AbsoluteJarPath(path: String) extends AbsoluteJarPathT with NonStorableJarPath
private[optimus] final case class MissingJarPath(path: String) extends MissingJarPathT with NonStorableJarPath
