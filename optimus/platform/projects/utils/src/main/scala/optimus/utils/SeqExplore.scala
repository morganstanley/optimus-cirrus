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
package optimus.utils
import scala.annotation.tailrec

/**
 * Utility for searching along sequences
 * @param s
 * @tparam T
 */
class SeqExplore[T] private (s: Seq[T]) {

  private def continueWith(s: Seq[T]) = new SeqExplore(s)

  /**
   * Search until the predicate matches.
   * @param f
   * @return
   */
  @tailrec final def until(f: T => Boolean): SeqExplore[T] = {
    s match {
      case Seq() =>
        this
      case x +: xs if (f(x)) =>
        this
      case _ +: xs =>
        continueWith(xs).until(f)
    }
  }

  /**
   * Search until the predicate doesn't match.
   * @param f
   * @return
   */
  final def untilNot(f: T => Boolean): SeqExplore[T] = until(!f(_))

  /**
   * Find the first element past matches of the predicate.
   * @param f
   * @return
   */
  final def past(f: T => Boolean): SeqExplore[T] = until(f).untilNot(f)

  /**
   * Skip some arbitrary number of elements.
   * @param i
   * @return
   */
  @tailrec final def skip(i: Int): SeqExplore[T] = {
    if (i <= 0 || s.isEmpty)
      this
    else
      continueWith(s.tail).skip(i - 1)
  }

  final def next: SeqExplore[T] = skip(1)

  def apply(g: SeqExplore[T] => SeqExplore[T]) = g(this)

  def orElse(se: => SeqExplore[T]) = if (s.isEmpty) se else this

  /**
   * Extract the sought element if available.
   * @return
   */
  final def get: Option[T] = s.headOption
}

object SeqExplore {
  def apply[T](s: Seq[T]) = new SeqExplore(s)
}
