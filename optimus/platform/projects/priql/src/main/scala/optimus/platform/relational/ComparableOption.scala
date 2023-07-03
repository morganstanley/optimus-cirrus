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
package optimus.platform.relational

class ComparableOptions {
  def <(a: Option[Any], b: Option[Any]): Boolean = {
    (a, b) match {
      case (None, None) => false
      case (None, _)    => true
      case (_, None)    => false
      case _ => {
        (a.get, b.get) match {
          case (l: Int, r: Int)               => l < r
          case (l: Short, r: Short)           => l < r
          case (l: Long, r: Long)             => l < r
          case (l: Byte, r: Byte)             => l < r
          case (l: Char, r: Char)             => l < r
          case (l: Boolean, r: Boolean)       => l < r
          case (l: Double, r: Double)         => l < r
          case (l: Float, r: Float)           => l < r
          case (l: String, r: String)         => l < r
          case (l: BigInt, r: BigInt)         => l < r
          case (l: BigDecimal, r: BigDecimal) => l < r
          case (l: Option[_], r: Option[_])   => <(l, r)
          case (l, r) => throw new IllegalArgumentException(s"mismatched types ${l.getClass} and ${r.getClass}")
        }
      }
    }
  }

  def >(a: Option[Any], b: Option[Any]): Boolean = {
    (a, b) match {
      case (None, None) => false
      case (None, _)    => false
      case (_, None)    => true
      case _ => {
        (a.get, b.get) match {
          case (l: Int, r: Int)               => l > r
          case (l: Short, r: Short)           => l > r
          case (l: Long, r: Long)             => l > r
          case (l: Byte, r: Byte)             => l > r
          case (l: Char, r: Char)             => l > r
          case (l: Boolean, r: Boolean)       => l > r
          case (l: Double, r: Double)         => l > r
          case (l: Float, r: Float)           => l > r
          case (l: String, r: String)         => l > r
          case (l: BigInt, r: BigInt)         => l > r
          case (l: BigDecimal, r: BigDecimal) => l > r
          case (l: Option[_], r: Option[_])   => >(l, r)
          case (l, r) => throw new IllegalArgumentException(s"mismatched types ${l.getClass} and ${r.getClass}")
        }
      }
    }
  }
}
