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
package optimus.platform.dal

/**
 * Can be viewed as a series of discrete (unitary, irreducible) values. Provides ways of traversing that series given a
 * member, for example the [[Discrete#successor]] and [[Discrete#predecessor]] methods
 *
 * @tparam T
 *   The type of element in the discrete series
 */
trait Discrete[T] {

  /**
   * Returns the next irreducible, unitary value in the discrete series
   *
   * For example, one would expect that calling
   *
   * @param t
   *   A member of the discrete series type `T`
   * @return
   *   The immediate successor of `t` in the series
   */
  def successor(t: T): T

  /**
   * Returns the previous irreducible, unitary value in the discrete series
   *
   * @param t
   *   A member of the discrete series type `T`
   * @return
   *   The immediate predecessor of `t` in the series
   */
  def predecessor(t: T): T
}
