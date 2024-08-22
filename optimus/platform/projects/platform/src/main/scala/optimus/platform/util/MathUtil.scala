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
package optimus.platform.util

import scala.collection.immutable.Range.BigDecimal
import java.math.BigDecimal.{valueOf => toBD}

object MathUtil {

  /**
   * A replacement to the deprecated Range.Double. This is slower, but more precise than the equivalent deprecated 'to'
   * method. n.b. because range is calculated as BigDecimal 1,10,1 with include 10 ('to' might skip the final value, use
   * doubleRangeExclusive for this behaviour) This is a known limitation due to double precision conversion. Returned as
   * a list rather than a Range for simplicity
   */
  def doubleRangeInclusive(start: Double, end: Double, step: Double): List[Double] =
    BigDecimal.inclusive(toBD(start), toBD(end), toBD(step)).toList.map(_.doubleValue)

  /**
   * Exclusive version of doubleRange 1 to 10 step 1 will not include 10
   */
  def doubleRangeExclusive(start: Double, end: Double, step: Double): List[Double] =
    BigDecimal(toBD(start), toBD(end), toBD(step)).toList.map(_.doubleValue)
}
