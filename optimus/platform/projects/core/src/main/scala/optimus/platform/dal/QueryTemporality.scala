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

import java.time.Instant
import optimus.platform.TimeInterval
import optimus.platform.ValidTimeInterval

object QueryTemporality {
  final case class At(validTime: Instant, txTime: Instant) extends QueryTemporality
  final case class ValidTime(validTime: Instant) extends QueryTemporality
  final case class TxTime(txTime: Instant) extends QueryTemporality
  final case class BitempRange(vtRange: ValidTimeInterval, ttRange: TimeInterval, inRange: Boolean)
      extends QueryTemporality
  case object All extends QueryTemporality
}

sealed trait QueryTemporality
