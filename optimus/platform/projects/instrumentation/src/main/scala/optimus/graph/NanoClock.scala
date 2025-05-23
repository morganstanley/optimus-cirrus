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

import optimus.graph.diagnostics.ProfiledEvent

/** A clock which tells the (possibly fake, possibly real) time in nano seconds */
trait NanoClock {
  def nanoTime: Long

  // for synchronising times between threads
  def publishEvent(eventID: Int, state: ProfiledEvent): Unit = ()
  def consumeEvent(eventID: Int, state: ProfiledEvent): Unit = ()
  def consumeTimeInMs(ms: Long, topic: NanoClockTopic): Unit = ()
}

trait NanoClockTopic
private[graph] object CacheTopic extends NanoClockTopic
