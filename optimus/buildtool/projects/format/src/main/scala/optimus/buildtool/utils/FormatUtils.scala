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
package optimus.buildtool.utils

import scala.collection.immutable.Seq

object FormatUtils {

  /**
   * Like Seq#distinct except that it preserves only the last occurrence of each entry rather than only the first.
   *
   * This is useful for classpath ordering, because if for example:
   *
   *   - A depends on C and B
   *   - B depends on C, and monkey patches it
   *
   * Then a depth first traversal yields A C B C, which distincts to A C B, thus losing the monkey patch in B. If we
   * instead distinctLast, we get A B C. In general, every entry will appear in its lowest precedence, allowing anyone
   * who depends on it (and may therefore patch it) to go first.
   */
  def distinctLast[A](s: Seq[A]): Seq[A] = s.toVector.reverse.distinct.reverse
}
