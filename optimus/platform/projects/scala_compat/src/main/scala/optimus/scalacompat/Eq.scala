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
package optimus.scalacompat

object Eq {

  /**
   * Like Scala's `==`, except Double.NaN == Double.NaN.
   *
   * It's a complicated situation but basically - for Java:
   *   1. `NaN != NaN`
   *   1. `NaN.equals(NaN)`
   *
   * For Scala:
   *   1. `NaN != NaN`
   *   1. `NaN.equals(NaN)` (of course, it's the same JDK method implementation)
   *   1. `NaN.asInstanceOf[AnyRef] ne NaN.asInstanceOf[AnyRef]`
   *
   * If you know the values don't include Double (or NaN specially), use `==`.
   *
   * Otherwise, for instance in completely generic contexts, you may prefer using this.
   */
  def eql(a: Any, b: Any): Boolean =
    a match {
      case a: Double if a.isNaN =>
        b match {
          case b: Double if b.isNaN => true
          case _                    => a == b
        }
      case _ => a == b
    }
}
