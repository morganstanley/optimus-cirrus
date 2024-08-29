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
package optimus.platform.dal.messages

import optimus.platform.dal.messages.MessagesSubscriptionType.MessagesSubscriptionType

import java.util.concurrent.atomic.AtomicInteger
import optimus.platform.relational.tree._

object MessagesSubscriptionType {
  sealed trait MessagesSubscriptionType
  case object ContainedEvent extends MessagesSubscriptionType
  case object UpsertableTransaction extends MessagesSubscriptionType
  case object Streamable extends MessagesSubscriptionType
}

/**
 * This class represents "messages" subscription for DAL messaging system (using Kafka).
 *   - ContainedEvent
 *   - UpsertableTransaction
 */
final case class MessagesSubscription private (
    subId: MessagesSubscription.Id,
    eventClassName: MessagesSubscription.EventClassName,
    subscriptionType: MessagesSubscription.SubscriptionType
) {

  // TODO (OPTIMUS-49663): Support client-side priql filter when subscribing to an @event(contained=true)
  def hasClientSideFilter: Boolean = false
}

object MessagesSubscription {
  type Id = Int
  type EventClassName = String
  type SubscriptionType = MessagesSubscriptionType

  private val subIdGenerator = new AtomicInteger(0)

  def apply(
      className: EventClassName,
      subscriptionType: SubscriptionType
  ): MessagesSubscription = MessagesSubscription(
    subId = subIdGenerator.incrementAndGet(),
    eventClassName = className,
    subscriptionType = subscriptionType
  )

  def containedEvent(
      eventClassName: EventClassName
  ): MessagesSubscription = MessagesSubscription(
    subId = subIdGenerator.incrementAndGet(),
    eventClassName = eventClassName,
    subscriptionType = MessagesSubscriptionType.ContainedEvent
  )

  def containedEvent(
      element: MultiRelationElement
  ): MessagesSubscription = {
    element match {
      case MethodElement(QueryMethod.WHERE, List(_, MethodArg(_, FuncElement(_, _, _))), _, _, _) =>
        throw new NotImplementedError(
          "Client-side priql filter when subscribing to an @event(contained=true) is yet to be Supported!")
      case _ => // Do Nothing
    }
    containedEvent(eventClassName = element.rowTypeInfo.name)
  }

  def upsertableTransaction(
      className: EventClassName
  ): MessagesSubscription = MessagesSubscription(
    subId = subIdGenerator.incrementAndGet(),
    eventClassName = className,
    subscriptionType = MessagesSubscriptionType.UpsertableTransaction
  )

  def upsertableTransaction(
      element: MultiRelationElement
  ): MessagesSubscription = {
    element match {
      case MethodElement(QueryMethod.WHERE, List(_, MethodArg(_, FuncElement(_, _, _))), _, _, _) =>
        throw new NotImplementedError(
          "Client-side priql filter when subscribing to an @stored @entity published via UpsertableTransaction is yet to be supported!")
      case _ => // Do Nothing
    }
    upsertableTransaction(className = element.rowTypeInfo.name)
  }

  def streamingTransaction(
      className: EventClassName
  ): MessagesSubscription = MessagesSubscription(
    subId = subIdGenerator.incrementAndGet(),
    eventClassName = className,
    subscriptionType = MessagesSubscriptionType.Streamable
  )

  def streamingTransaction(
      element: MultiRelationElement
  ): MessagesSubscription = {
    element match {
      case MethodElement(QueryMethod.WHERE, List(_, MethodArg(_, FuncElement(_, _, _))), _, _, _) =>
        throw new NotImplementedError(
          "Client-side priql filter when subscribing to an @stored @entity published via StreamingTransaction is yet to be supported!")
      case _ => // Do Nothing
    }
    streamingTransaction(className = element.rowTypeInfo.name)
  }
}
