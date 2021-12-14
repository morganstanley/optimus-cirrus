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

class Patches(patches: Array[Patch], val source: SourceFile, encoding: Charset) {

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
  private def underlyingFile: Path =
    if (source.file.getClass.getName.endsWith("xsbt.ZincVirtualFile")) {
      val path = source.file.asInstanceOf[{ def underlying(): { def id(): String } }].underlying().id()
      Paths.get(path)
    } else {
      source.file.file.toPath
    }
}
object Patches {

  def apply(patches: Seq[Patch], source: SourceFile, encoding: Charset) = {
    val patchesArray = patches.toArray
    util.Arrays.sort(patchesArray, Ordering.by[Patch, Int](_.span.start))
    def patchesOverlap: Option[(Patch, Patch)] = {
      if (patchesArray.nonEmpty) {
        patchesArray.reduceLeft { (p1, p2) =>
          if (p1.span.end > p2.span.start) {
            return Some((p1, p2))
          }
          p2
        }
      }
      None
    }
    patchesOverlap match {
      case Some((p1, p2)) => throw new IllegalArgumentException("patches overlap: " + p1 + " " + p2)
      case None           =>
    }
    new Patches(patchesArray, source, encoding)
  }
}
