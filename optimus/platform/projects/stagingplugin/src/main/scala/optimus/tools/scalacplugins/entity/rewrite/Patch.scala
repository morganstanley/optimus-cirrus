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
package optimus.tools.scalacplugins
package entity
package rewrite

import java.nio.charset.Charset
import java.nio.file.Path
import java.util

import scala.annotation.tailrec
import scala.reflect.internal.util.Position
import scala.reflect.internal.util.SourceFile
import java.nio.file.Files
import java.nio.file.Paths

case class Patch(span: Position, replacement: String) {
  def delta: Int = replacement.length - (span.end - span.start)
}

class Patches(patches: Array[Patch], val source: SourceFile, underlyingFile: Path, encoding: Charset) {

  lazy val applied: String = {
    // not source.content, which sometimes includes a synthetic \n at the EOF
    val sourceChars = new String(Files.readAllBytes(underlyingFile), encoding).toCharArray

    val patchedChars = new Array[Char](sourceChars.length + patches.iterator.map(_.delta).sum)
    @tailrec def loop(pIdx: Int, inIdx: Int, outIdx: Int): Unit = {
      def copy(upTo: Int): Int = {
        val untouched = upTo - inIdx
        System.arraycopy(sourceChars, inIdx, patchedChars, outIdx, untouched)
        outIdx + untouched
      }
      if (pIdx < patches.length) {
        val p = patches(pIdx)
        val outNew = copy(p.span.start)
        p.replacement.copyToArray(patchedChars, outNew)
        loop(pIdx + 1, p.span.end, outNew + p.replacement.length)
      } else {
        val outNew = copy(sourceChars.length)
        assert(outNew == patchedChars.length, s"R$outNew != ${patchedChars.length}")
      }
    }
    loop(0, 0, 0)
    new String(patchedChars)
  }
  def overwriteSourceFile(): Unit = {
    val bytes = applied.getBytes(encoding)
    Files.write(underlyingFile, bytes)
  }
}
object Patches {
  def underlyingFile(source: SourceFile): Option[Path] = {
    val fileClass = source.file.getClass.getName
    if (fileClass.endsWith("xsbt.ZincVirtualFile")) {
      val path = source.file.asInstanceOf[{ def underlying(): { def id(): String } }].underlying().id()
      val p = Paths.get(path)
      // Generated source file don't have physical files on disk
      Some(p).filter(Files.exists(_))
    } else if (fileClass.endsWith("OptimusStringVirtualFile"))
      None
    else
      Some(source.file.file.toPath)
  }

  /** Source code at a position. Either a line with caret (offset), else the code at the range position. */
  def codeOf(pos: Position, source: SourceFile): String =
    if (pos.start < pos.end) new String(source.content.slice(pos.start, pos.end))
    else {
      val line = source.offsetToLine(pos.point)
      val code = source.lines(line).next()
      val caret = " " * (pos.point - source.lineToOffset(line)) + "^"
      s"$code\n$caret"
    }

  private def checkNoOverlap(patches: Array[Patch], source: SourceFile): Boolean = {
    var ok = true
    for (Array(p1, p2) <- patches.sliding(2) if p1.span.end > p2.span.start) {
      ok = false
      val msg = s"""
                   |overlapping patches;
                   |
                   |add `${p1.replacement}` at
                   |${codeOf(p1.span, source)}
                   |
                   |add `${p2.replacement}` at
                   |${codeOf(p2.span, source)}""".stripMargin.trim
      throw new IllegalArgumentException(msg)
    }
    ok
  }

  def apply(patches: Array[Patch], source: SourceFile, underlyingFile: Path, encoding: Charset): Patches = {
    util.Arrays.sort(patches, Ordering.by[Patch, Int](_.span.start))
    checkNoOverlap(patches, source)
    new Patches(patches, source, underlyingFile, encoding)
  }
}
