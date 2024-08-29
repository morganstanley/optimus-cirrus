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
package optimus.platform.dal.pubsub

import optimus.dsi.partitioning.Partition
import optimus.dsi.partitioning.PartitionMap
import optimus.dsi.pubsub.ExpressionQueryHolder
import optimus.dsi.pubsub.HeartbeatQueryHolder
import optimus.dsi.pubsub.Subscription
import optimus.platform.dsi.expressions.Entity
import optimus.platform.dsi.expressions.Expression
import optimus.platform.dsi.expressions.Select

object PubSubHelper {
  implicit class SubscriptionEnhancement(subscription: Subscription)(implicit partitionMap: PartitionMap) {
    private def getClassName(expr: Expression): String = {
      expr match {
        case select: Select =>
          select.from match {
            case e: Entity => e.name
            case _         => throw new IllegalArgumentException(s"Only Entity based priql expression supported.")
          }
        case _ =>
          throw new IllegalArgumentException(s"Unsupported priql expression: $expr. Only 'Select' is supported.")
      }
    }

    lazy val inferredPartition: Partition = {
      subscription.query match {
        case holder: ExpressionQueryHolder =>
          val className = getClassName(holder.query)
          val partition = partitionMap.partitionForType(className)
          partition
        case HeartbeatQueryHolder(partition, _) =>
          require(partitionMap.allPartitions.contains(partition), s"attempt to access unavailable partition $partition")
          partition
      }
    }
  }

  implicit class SubscriptionSetEnhancement(subSet: Set[Subscription])(implicit partitionMap: PartitionMap) {
    lazy val inferredPartition: Partition = {
      require(subSet.nonEmpty, "The subscription set must be non-empty")
      val g = subSet.groupBy(_.inferredPartition)
      require(g.size == 1, "All subscriptions should be of the same partition")
      g.keys.head
    }
  }
}
