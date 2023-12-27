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
package optimus.buildtool.utils.stats

import java.io.File
import java.nio.file.Path

import msjava.slf4jutils.scalalog.getLogger
import optimus.platform._

import scala.jdk.CollectionConverters._

/**
 * Represents a compilation run path broken down in to the standard codetree components
 */
final case class CompilationRun(meta: String, bundle: String, module: String, srcType: String) {
  def merge(c: CompilationRun): CompilationRun = CompilationRun(
    meta = if (meta == c.meta) meta else "",
    bundle = if (bundle == c.bundle) bundle else "",
    module = if (module == c.module) module else "",
    srcType = if (srcType == c.srcType) srcType else ""
  )

  def withoutSrcType: CompilationRun = copy(srcType = "")
}

object CompilationRun {
  private val log = getLogger(getClass)
  implicit val ordering: math.Ordering[CompilationRun] =
    Ordering.by((cr: CompilationRun) => (cr.meta, cr.bundle, cr.module, cr.srcType))

  def fromOutputDir(outputDir: String): CompilationRun = {
    // e.g.: .../build/perf/scala/TEMP6ee969b5-7152-433a-b8be-a284fc8c2ece-optimus.platform.git-utils.main.HASH4ee9e53b761ad192a92faf195c3c4c2ad09ff82204e9439440aaf5a02cd0ec0f.jar
    val fileName = new File(outputDir).getName
    fileName.split('.') match {
      case Array(tempHashWithMeta, bundle, module, srcType, fingerprint, jar) =>
        val meta = tempHashWithMeta.split('-').drop(5).mkString("-")
        CompilationRun(meta = meta, bundle = bundle, module = module, srcType = srcType)
      case _ =>
        throw new IllegalArgumentException(
          s"Couldn't parse output dir $outputDir to identify meta, bundle, module, and srcType")
    }

  }

  def fromRelativeSourcePath(path: Path): Option[CompilationRun] = {
    val parts: Seq[Path] = path.iterator().asScala.toIndexedSeq
    if (parts.size >= 5) {
      val meta = parts(0).toString
      val bundle = parts(1).toString
      // a few projects (ex: tools)  have the module directory directly inside the bundle, whereas others have a
      // projects directory in between
      val (module, srcType) =
        if (bundle == "risk" || bundle == "tools") (parts(2).toString, parts(3).toString)
        else (parts(3).toString, parts(4).toString)
      Some(CompilationRun(meta, bundle, module, srcType))
    } else {
      log.warn(s"Ignoring unexpectedly short source file path : $path")
      None
    }
  }

  def fromRelativeJarPath(path: Path): CompilationRun = {
    // e.g.: optimus\platform\local\install\common\lib\git-utils.jar
    val parts: Seq[String] = path.iterator().asScala.toSeq.map(_.toString)
    parts match {
      case Seq(meta, bundle, local, install, common, lib, jarName) =>
        val (module, srcType) = decomposeJarName(jarName)
        CompilationRun(meta = meta, bundle = bundle, module = module, srcType = srcType)
      case _ =>
        throw new IllegalArgumentException(
          s"Couldn't parse relative path $path to identify meta, bundle, module, and srcType")
    }
  }

  private def decomposeJarName(jarName: String): (String, String) = {
    // e.g.: git-utils.jar, git-utils.test.jar
    jarName.split('.') match {
      case Array(module, jar)          => module -> "main"
      case Array(module, srcType, jar) => module -> srcType
      case _ => throw new IllegalArgumentException(s"Couldn't decompose jar name $jarName into module and srcType")
    }
  }
}
