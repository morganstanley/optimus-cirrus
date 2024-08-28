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

sealed trait TimeoutResult[+T] {
  def completedInTime: Boolean
  def timedOut: Boolean = !completedInTime
}

final case class CompletedInTime[+T](result: T) extends TimeoutResult[T] {
  override def completedInTime = true
}

final case class TimedOut(timeoutMs: Long) extends TimeoutResult[Nothing] {
  override def completedInTime = false
}

object TimeoutResult {
  def apply[T](timeoutMs: Long)(option: Option[T]): TimeoutResult[T] =
    option.map(CompletedInTime(_)).getOrElse(TimedOut(timeoutMs))
}
