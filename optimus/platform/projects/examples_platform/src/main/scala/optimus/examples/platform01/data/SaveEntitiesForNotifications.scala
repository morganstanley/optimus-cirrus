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
package optimus.examples.platform01.data

import optimus.platform._

package fornotifications {
  @event class NotificationEvent(val id: Int)
  @stored @entity class NotificationEntity1(@key val id: Int, @indexed val name2: String)
  @stored @entity class NotificationEntity2(@key val name: String, val rank: Int)
}

object SaveEntitiesForNotifications extends LegacyOptimusApp[OptimusAppCmdLine] {
  import fornotifications._

  // DAL.obliterateEventClass[NotificationEvent]
  // DAL.obliterateClass[NotificationEntity1]
  // DAL.obliterateClass[NotificationEntity2]

  for (i <- 1 to 10) {
    val e1 = NotificationEntity1(i, "trade" + i)
    val e2 = NotificationEntity2("Player" + i, i)
    given(validTimeAndTransactionTimeNow) {
      val evt = NotificationEvent.uniqueInstance(i)
      newEvent(evt) {
        DAL.upsert(e1)
        DAL.upsert(e2)
      }
    }
  }

}
