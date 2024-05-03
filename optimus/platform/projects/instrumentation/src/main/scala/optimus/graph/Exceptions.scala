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
package optimus.graph

import scala.collection.compat._
import scala.collection.mutable

object Exceptions {

  /**
   * Turn a Throwable into an iterator lazily walking the .getCause chain.
   */
  def causes(t: Throwable): Iterator[Throwable] = new Iterator[Throwable] {
    private var current = t
    override def hasNext: Boolean = current != null
    override def next(): Throwable = {
      if (current != null) {
        val out = current
        current = current.getCause
        out
      } else throw new NoSuchElementException("no more causes!")
    }
  }

  /**
   * Return an iterator over the stack trace lines of a throwable, with FQCN minimized.
   */
  def stackTraceLines(e: Throwable): Iterator[String] = {
    val sb = new mutable.StringBuilder // temporary buffer
    Iterator.single(s"${e.getClass.getSimpleName}(${e.getMessage})") ++
      e.getStackTrace.iterator.map { elem =>
        val fqcn = elem.getClassName.split('.')
        for (p <- fqcn.dropRight(1)) {
          sb += p.charAt(0)
          sb += '.'
        }
        sb ++= s"${fqcn.last}.${elem.getMethodName}"
        sb ++= s"(${elem.getFileName}:${elem.getLineNumber})"
        val out = sb.toString()
        sb.clear()
        out
      }

  }

  def minimizeTrace(t: Throwable, maxDepth: Int, maxCauses: Int): collection.Seq[String] = {
    val lineIter = for {
      cause <- causes(t).take(maxCauses)
      lines <- stackTraceLines(cause).take(maxDepth + 1)
    } yield lines

    lineIter.to(Seq)
  }

}
