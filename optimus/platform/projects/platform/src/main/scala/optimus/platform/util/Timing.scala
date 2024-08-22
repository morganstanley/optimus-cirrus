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

import optimus.platform.AdvancedUtils

object Timing {
  final def timed[T](timingAction: Long => Unit)(op: => T): T = {
    val (time, result) = AdvancedUtils.timed(op)
    timingAction(time)
    result
  }
  final def timedLogged[T](log: String => Unit, actionDescription: String)(op: => T): T = {
    timed(nanos => log(s"${actionDescription} took ${nanos / 1000000}ms"))(op)
  }
}
