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

import java.util.concurrent.ConcurrentHashMap
import java.util.regex.Pattern
import optimus.buildtool.artifacts.AnalysisArtifactType
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.InternalClassFileArtifactType
import optimus.buildtool.artifacts.MessageArtifactType
import optimus.buildtool.config.NamingConventions._
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.FileInJarAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.JdkPlatformAsset
import optimus.buildtool.files.Pathed
import optimus.buildtool.files.RelativePath
import optimus.buildtool.trace.Java
import optimus.buildtool.trace.MessageTrace
import optimus.buildtool.trace.Scala
import sbt.internal.inc.Analysis

import java.nio.file.Paths
import scala.util.matching.Regex

private object ZincUtils {

  def classType(traceType: MessageTrace): InternalClassFileArtifactType =
    switch(traceType, ArtifactType.Scala, ArtifactType.Java)

  def messageType(traceType: MessageTrace): MessageArtifactType =
    switch(traceType, ArtifactType.ScalaMessages, ArtifactType.JavaMessages)

  def analysisType(traceType: MessageTrace): AnalysisArtifactType =
    switch(traceType, ArtifactType.ScalaAnalysis, ArtifactType.JavaAnalysis)

  private def switch[A](traceType: MessageTrace, scala: A, java: A): A = traceType match {
    case Scala => scala
    case Java  => java
    case x     => throw new IllegalArgumentException(s"Unexpected trace type: $x")
  }

  private val sepReText = """[/\\]"""
  private val tpeReText = ArtifactType.known.map(_.name).mkString("(", "|", ")")

  private val uuidReText = "[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"

  private val fnameReText =
    s"(?:TEMP($uuidReText)-)?([\\w\\.\\-]+?)\\.($EMPTYHASH|$HASH[0-9a-f]+[ZM]*)\\.(\\w+)(!\\S+)?"
  private val fnameRe = ("^" + fnameReText + "$").r

  // Do a little Path -> String -> Path dance here to work around the fact that (on Windows) dummyOutputPath
  // is a relative path
  val dummyFile = FileAsset(Paths.get(Pathed.pathString(Analysis.dummyOutputPath)))

  /**
   * Splits path into Some((tpe, Some(tmp), name, hash, Some(inner))
   */
  final case class Dissection(
      jar: JarAsset,
      tpe: ArtifactType,
      uuid: Option[String],
      name: String,
      hash: String,
      suffix: String,
      fileWithinJar: Option[RelativePath]
  ) {
    def withClass(f: RelativePath): Dissection = copy(fileWithinJar = Some(f))
  }

  // Permanently cache jar paths, without possible inner classpath
  private val pathDissectionCache = new ConcurrentHashMap[JarAsset, Dissection]
  private def dissectPathInternal(jar: JarAsset): Dissection =
    pathDissectionCache.computeIfAbsent(
      jar,
      { jarPath =>
        val jarName = jarPath.name
        val tpe = ArtifactType.parse(jarPath.parent.name)
        jarName match {
          case fnameRe(uuid, name, hash, ext, _) =>
            Dissection(jarPath, tpe, Option(uuid), name, hash, ext, None)
          case _ =>
            throw new RuntimeException(s"$jarName did not match BUILD/${tpe.name}/$fnameRe")
        }
      }
    )

  private[zinc] def dissectFile(buildDir: Directory, file: Pathed): Either[Pathed, Dissection] = {
    def okDir(j: JarAsset) = buildDir.contains(j)
    file match {
      case p: RelativePath => Left(p) // eg. foo/bar/Baz.scala
      case d: Directory    => Left(d) // eg. //afs/path/to/msjava/PROJ/azulzulu-openjdk/11.0.7/exec
      case m: JdkPlatformAsset =>
        Left(m) // eg. //modules/java.base/java/lang/Object.class or //87/java.base/java/lang/Object.sig
      case j: JarAsset       => Either.cond(okDir(j), dissectPathInternal(j), j)
      case f: FileInJarAsset => Either.cond(okDir(f.jar), dissectPathInternal(f.jar).withClass(f.file), f)
      // Zinc (from 1.10.2) tries to work out if it's compiling to a jar or not by inspecting the output path,
      // forgetting that zinc itself sets the output path to `Analysis.dummyOutputPath` to make analysis files
      // more machine independent. Don't do anything clever here - just return the dummy path that zinc gave us.
      case f: FileAsset if f == dummyFile => Left(f)
      case x                              => throw new IllegalArgumentException(s"Unexpected file $x")
    }
  }

  def reconstructFile(
      buildDir: Directory,
      tpe: ArtifactType,
      uuid: Option[String],
      name: String,
      hash: String,
      fileInJar: Option[RelativePath]
  ): FileAsset = {
    val t = uuid.map(u => s"TEMP$u-").getOrElse("")
    val jarName = s"$t$name.$hash.jar"
    val p = buildDir.resolveDir(tpe.name)
    val jar = p.resolveJar(jarName)
    fileInJar.map(f => FileInJarAsset(jar, f)).getOrElse(jar)
  }

  private val buildDirRe = new ConcurrentHashMap[Directory, Regex]()

  // replace all the substrings matching "<buildDir>/<tpe>/<file-format> in text, and return as JarAssets
  def replaceBuildJarsInText(buildDir: Directory, text: String)(f: JarAsset => String): String = {
    val buildDirStr = buildDir.pathString
    if (!text.contains(buildDirStr)) text
    else {
      val re = buildDirRe.computeIfAbsent(
        buildDir,
        _ => (Pattern.quote(buildDir.pathString) + sepReText + tpeReText + sepReText + fnameReText).r
      )
      re.replaceAllIn(text, (m: Regex.Match) => f(JarAsset(buildDir.fileSystem.getPath(m.matched))))
    }
  }

  def substringAfterLast(str: String, ch: Char): String = {
    val idx = str.lastIndexOf(ch)
    if (idx < 0) str else str.substring(idx + 1)
  }
}
