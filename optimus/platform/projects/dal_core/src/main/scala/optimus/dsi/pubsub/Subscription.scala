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
package optimus.dsi.pubsub

import java.util.concurrent.atomic.AtomicInteger

import optimus.dsi.partitioning.DefaultPartition
import optimus.dsi.partitioning.Partition
import optimus.entity.EntityInfoRegistry
import optimus.graph.DiagnosticSettings
import optimus.platform._
import optimus.platform.dsi.bitemporal.DSIQueryTemporality
import optimus.platform.dsi.expressions._
import optimus.platform.storable.EntityReference
import optimus.platform.storable.SerializedEntity

final case class Subscription private (
    subId: Subscription.Id,
    query: QueryHolder,
    includeSow: Boolean,
    entitledOnly: Boolean
) {

  def className: Option[SerializedEntity.TypeRef] = query match {
    case ExpressionQueryHolder(expr, _) =>
      expr match {
        case select: Select =>
          select.from match {
            case e: Entity => Some(e.name)
            case _         => None
          }
        case _ => None
      }
    case HeartbeatQueryHolder(_, _) => None
  }

  def isHeartbeat: Boolean = query match {
    case HeartbeatQueryHolder(_, _) => true
    case _                          => false
  }

  lazy val queryString: String = query match {
    case ExpressionQueryHolder(expr, _) => ExpressionPriQLFormatter.format(expr)
    case HeartbeatQueryHolder(_, _)     => s"heartbeat"
  }

  def hasClientSideFilter: Boolean = clientFilter.isDefined

  def clientFilter: Option[NodeFunction1[Any, Boolean]] = query match {
    case HeartbeatQueryHolder(_, _)   => None
    case ExpressionQueryHolder(_, cf) => cf
  }

  override def toString: String =
    s"(subId=$subId, sow=$includeSow, entitledOnly=$entitledOnly, query=$queryString)"
}

object Subscription {
  type Id = Int
  private val subIdGenerator = new AtomicInteger(0)
  val defaultHeartbeatIntervalInMillis: Long =
    DiagnosticSettings.getLongProperty("optimus.dsi.pubsub.defaultHeartbeatIntervalInMillis", 1000L)
  def apply(
      query: Expression,
      clientFilter: Option[NodeFunction1[Any, Boolean]],
      includeSow: Boolean,
      entitledOnly: Boolean
  ): Subscription =
    Subscription(subIdGenerator.incrementAndGet(), ExpressionQueryHolder(query, clientFilter), includeSow, entitledOnly)
  def apply(
      query: Expression,
      includeSow: Boolean,
      entitledOnly: Boolean = false
  ): Subscription = apply(query, None, includeSow, entitledOnly)
  def apply(): Subscription = apply(DefaultPartition)
  def apply(heartbeatIntervalInMillis: Long): Subscription = apply(DefaultPartition, heartbeatIntervalInMillis)
  def apply(partition: Partition): Subscription = apply(partition, defaultHeartbeatIntervalInMillis)
  def apply(partition: Partition, heartbeatIntervalInMillis: Long): Subscription = {
    Subscription(
      subId = subIdGenerator.incrementAndGet(),
      query = HeartbeatQueryHolder(partition, heartbeatIntervalInMillis),
      includeSow = false,
      entitledOnly = false)
  }

  def apply(ref: EntityReference, src: Class[_]): Subscription = {
    Subscription(
      subId = subIdGenerator.incrementAndGet(),
      query = ExpressionQueryHolder(
        createExpression(ref, src),
        None
      ),
      includeSow = false,
      entitledOnly = false
    )
  }

  def createExpression(ref: EntityReference, src: Class[_]): Select = createExpression(ref, src.getName)

  private def createExpression(ref: EntityReference, clsName: SerializedEntity.TypeRef) = {
    val info = EntityInfoRegistry.getClassInfo(clsName)
    val parentEntities: Seq[String] = (info.baseTypes - info).iterator.map { _.runtimeClass.getName }.toIndexedSeq
    val id = Id() // should avoid use of EmptyId
    Select(
      from = Entity(
        name = clsName,
        DSIQueryTemporality.At(TimeInterval.Infinity, TimeInterval.Infinity),
        parentEntities,
        id = id),
      where = Some(
        Binary(BinaryOperator.Equal, Property.special(id, PropertyLabels.EntityRef), Constant(ref, TypeCode.Reference)))
    )
  }

  def extractEntityReference(select: Expression): Option[(EntityReference, SerializedEntity.TypeRef)] = {
    select match {
      case Select(
            Entity(name, _, _, _),
            Nil,
            Some(
              Binary(
                BinaryOperator.Equal,
                Property(PropertyType.Special, Seq(PropertyLabels.EntityRef), _),
                Constant(ref: EntityReference, TypeCode.Reference))),
            Nil,
            Nil,
            None,
            None,
            false,
            false,
            _) =>
        Some((ref, name))
      case _ => None
    }
  }

}

sealed trait QueryHolder
final case class ExpressionQueryHolder(query: Expression, clientFilter: Option[NodeFunction1[Any, Boolean]])
    extends QueryHolder
final case class HeartbeatQueryHolder(
    partition: Partition,
    intervalInMillis: Long = Subscription.defaultHeartbeatIntervalInMillis)
    extends QueryHolder
