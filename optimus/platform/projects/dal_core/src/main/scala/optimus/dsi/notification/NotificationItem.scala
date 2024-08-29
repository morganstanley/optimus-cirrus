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
package optimus.dsi.notification

import java.time.Instant
import optimus.dsi.partitioning.Partition
import optimus.platform.dsi.bitemporal.Context

sealed trait NotificationItem {
  def txTime: Instant

  def context: Context

  def primarySeq: Long

  def partition: Partition
}

final case class NotificationMessageWrapper(message: NotificationMessage) extends NotificationItem {
  def txTime: Instant = message.txTime

  def context: Context = message.context

  def primarySeq: Long = message.primarySeq

  def partition: Partition = message.partition
}

final case class NotificationTransaction(
    id: String,
    txTime: Instant,
    context: Context,
    primarySeq: Long,
    messages: Seq[NotificationEntry],
    partition: Partition)
    extends NotificationItem {
  def begin = BeginTransaction(txTime, context, primarySeq, partition)

  def end = EndTransaction(txTime, context, primarySeq, partition)
}
