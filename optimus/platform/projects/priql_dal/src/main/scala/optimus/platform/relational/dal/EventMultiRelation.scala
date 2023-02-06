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
package optimus.platform.relational.dal

import msjava.slf4jutils.scalalog
import optimus.platform._
import optimus.platform.dal.DalAPI
import optimus.platform.dal.EntityResolverReadImpl
import optimus.platform.dal.EventSerializer
import optimus.platform._
import optimus.platform.AsyncImplicits._
import optimus.platform.relational.EntitledOnlySupport
import optimus.platform.relational.RelationalException
import optimus.platform.relational.dal.internal.RawReferenceKey
import optimus.platform.relational.inmemory.ArrangeHelper
import optimus.platform.relational.inmemory.ArrangedSource
import optimus.platform.relational.inmemory.IterableSource
import optimus.platform.relational.persistence.protocol.ProviderPersistence
import optimus.platform.relational.persistence.protocol.ProviderPersistenceSupport
import optimus.platform.relational.tree._
import optimus.platform.storable.EventInfo

import scala.collection.mutable.ListBuffer

/**
 * support deserialization of from(event) only used in entitlement so far
 */
class EventStorableRelationElement[T](rowTypeInfo: TypeInfo[T], key: RelationKey[_])
    extends ProviderRelation(rowTypeInfo, key)
    with ProviderPersistenceSupport {

  override def getProviderName: String = "EventProvider"
  override def serializerForThisProvider: ProviderPersistence = EventProviderPersistence

  override def prettyPrint(indent: Int, out: StringBuilder): Unit = {
    indentAndStar(indent, out)
    out ++= s"$serial Provider: '${getProviderName}'\n"
  }

  def entityClassName: String = rowTypeInfo.name

  override def makeKey(newKey: RelationKey[_]): EventStorableRelationElement[T] = {
    new EventStorableRelationElement(rowTypeInfo, newKey)
  }
}

trait EventRelationElement extends StorableClassElement {
  def getProviderName: String = "EventProvider"
  def eventClassName: String = rowTypeInfo.name
  def rowTypeInfo: TypeInfo[_]
}

class EventMultiRelation[T <: BusinessEvent](
    val eventInfo: EventInfo,
    rowTypeInfo: TypeInfo[T],
    key: RelationKey[_],
    pos: MethodPosition,
    val entitledOnly: Boolean = false)(implicit dalApi: DalAPI)
    extends ProviderRelation(rowTypeInfo, key, pos)
    with EventRelationElement
    with ProviderPersistenceSupport
    with ArrangedSource
    with IterableSource[T]
    with EntitledOnlySupport {

  import EventMultiRelation._

  override def prettyPrint(indent: Int, out: StringBuilder): Unit = {
    indentAndStar(indent, out)
    out ++= s"$serial Provider: '$getProviderName' of type ${eventInfo.runtimeClass.getName}\n"
  }

  override def fillQueryExplainItem(level_id: Integer, table: ListBuffer[QueryExplainItem]): Unit = {
    table += new QueryExplainItem(level_id, getProviderName, s"${eventInfo.runtimeClass.getName}", "Event", 0)
  }

  def classInfo = eventInfo

  @async def getReadTxTime = dalApi.loadContext.ttContext.getTTForEvent(eventInfo.runtimeClass.getName)

  override def serializerForThisProvider: ProviderPersistence = EventProviderPersistence

  override def isSyncSafe = false
  override def getSync() = throw new RelationalException("EventProvider cannot execute query sync")

  @async override def get(): Iterable[T] = {
    logger.info("Load all BusinessEvent {}  from DAL to client.", eventInfo.runtimeClass)
    val ctx = dalApi.loadContext
    val cls = eventInfo.runtimeClass.getName
    val resolver = EvaluationContext.env.entityResolver.asInstanceOf[EntityResolverReadImpl]
    val middleResult = Query.Tracer.trace(pos, asNode(() => resolver.findEventsRaw(cls, ctx, entitledOnly))).apar map {
      EventSerializer.deserializeBusinessEvent(ctx)(_)
    }
    val res = ArrangeHelper.arrange(middleResult, RawReferenceKey[BusinessEvent]((t: BusinessEvent) => t.dal$eventRef))
    res.asInstanceOf[Iterable[T]]
  }

  override def makeKey(newKey: RelationKey[_]) = new EventMultiRelation[T](eventInfo, rowTypeInfo, newKey, pos)
  override def copyAsEntitledOnly() =
    new EventMultiRelation(eventInfo, rowTypeInfo, key, pos, true)
}

object EventMultiRelation {
  def apply[A <: BusinessEvent: TypeInfo](eventInfo: EventInfo, key: RelationKey[_], pos: MethodPosition)(implicit
      dalApi: DalAPI): EventMultiRelation[A] = {
    new EventMultiRelation[A](eventInfo, typeInfo[A], key, pos)
  }

  private lazy val logger = scalalog.getLogger[EventMultiRelation[_]]
}
