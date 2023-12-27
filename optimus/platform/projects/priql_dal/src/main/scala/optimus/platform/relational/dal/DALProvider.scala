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

import optimus.entity.ClassEntityInfo
import optimus.graph.Node
import optimus.platform._
import optimus.platform.annotations.internal.EmbeddableMetaDataAnnotation
import optimus.platform.annotations.internal.EntityMetaDataAnnotation
import optimus.platform.dal.DalAPI
import optimus.platform.dal.SessionFetcher
import optimus.platform.dsi.bitemporal.QueryPlan
import optimus.platform.pickling.IdentityUnpickler
import optimus.platform.pickling.OptionUnpickler
import optimus.platform.pickling.Unpickler
import optimus.platform.relational.EntitledOnlySupport
import optimus.platform.relational.KeyPropagationPolicy
import optimus.platform.relational.PriqlSettings
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.dal.accelerated.PicklerSelector
import optimus.platform.relational.dal.accelerated.ProjectedViewOnlyReducer
import optimus.platform.relational.dal.core.ExpressionQuery
import optimus.platform.relational.dal.core.ParameterReplacer
import optimus.platform.relational.dal.deltaquery.DALEntityBitemporalSpaceReducer
import optimus.platform.relational.dal.fullTextSearch.FullTextSearchOnlyReducer
import optimus.platform.relational.dal.sampling.DALSamplingReducer
import optimus.platform.relational.dal.serialization.DALFrom
import optimus.platform.relational.data.DataProvider
import optimus.platform.relational.data.FieldReader
import optimus.platform.relational.data.QueryCommand
import optimus.platform.relational.data.tree.ColumnElement
import optimus.platform.relational.data.tree.DALHeapEntityElement
import optimus.platform.relational.data.tree.OptionElement
import optimus.platform.relational.inmemory.ArrangedSource
import optimus.platform.relational.serialization._
import optimus.platform.relational.tree._
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityReference
import optimus.platform.storable.EntityReferenceHolder
import optimus.platform.storable.PersistentEntity
import optimus.platform.storable.VersionedReference

import java.time.Instant
import scala.util._

class DALProvider(
    val classEntityInfo: ClassEntityInfo,
    rowTypeInfo: TypeInfo[_],
    key: RelationKey[_],
    pos: MethodPosition,
    val dalApi: DalAPI,
    val canBeProjected: Boolean,
    val canBeFullTextSearch: Boolean,
    keyPolicy: KeyPropagationPolicy,
    val entitledOnly: Boolean = false)
    extends DataProvider(rowTypeInfo, key, pos, keyPolicy)
    with ArrangedSource
    with StorableClassElement
    with QueryBuilderConvertible
    with EntitledOnlySupport {
  import PriqlSettings._

  override def getProviderName = "DALProvider"

  override def reducersFor(category: ExecutionCategory): List[ReducerVisitor] = {
    def wrapped(r: ReducerVisitor) = new ProjectedViewOnlyReducer(r)
    def wrappedFullText(r: ReducerVisitor) = new FullTextSearchOnlyReducer(r)
    def mkDALReducer = if (this.supportsRegisteredIndexes) mkDALRegisteredIndexReducer else new core.DALReducer(this)
    def mkDALReferenceReducer =
      if (this.supportsRegisteredIndexes) mkDALRegisteredIndexReferenceReducer else new core.DALReferenceReducer(this)
    def mkAccReducer = new accelerated.DALAccReducer(this)
    def mkAccReferenceReducer = new accelerated.DALAccReferenceReducer(this)
    def mkDeltaReducer = new DALEntityBitemporalSpaceReducer(this)
    def mkSamplingReducer = new DALSamplingReducer(this)
    def mkDALRegisteredIndexReducer = new core.DALRegisteredIndexReducer(this)
    def mkDALRegisteredIndexReferenceReducer = new core.DALRegisteredIndexReferenceReducer(this)
    def mkDALFullTextSearchReducer = new fullTextSearch.DALFullTextSearchReducer(this)
    category match {
      case ExecutionCategory.Execute if enableProjectedPriql && enableFullTextSearchPriql =>
        List(mkAccReducer, mkDALFullTextSearchReducer, mkDALReducer)
      case ExecutionCategory.Execute if enableProjectedPriql      => List(mkAccReducer, mkDALReducer)
      case ExecutionCategory.Execute if enableFullTextSearchPriql => List(mkDALFullTextSearchReducer, mkDALReducer)
      case ExecutionCategory.Execute                              => List(wrapped(mkAccReducer), mkDALReducer)
      case ExecutionCategory.ExecuteReference if enableProjectedPriql =>
        List(mkAccReferenceReducer, mkDALReferenceReducer)
      case ExecutionCategory.ExecuteReference =>
        List(wrapped(mkAccReferenceReducer), mkDALReferenceReducer)
      case ExecutionCategory.QueryEntityBitemporalSpace => List(mkDeltaReducer)
      case DALExecutionCategory.Sampling                => List(mkSamplingReducer)
      case _ =>
        throw new IllegalArgumentException("Unsupported ExecutionCategory")
    }
  }

  override def prettyPrint(indent: Int, out: StringBuilder): Unit = {
    indentAndStar(indent, out)
    out ++= serial + " Provider:" + " '" + getProviderName + "' " + "of" + " type " + rowTypeInfo.runtimeClass.getName + "\n"
  }

  def classInfo = classEntityInfo

  override def makeKey(newKey: RelationKey[_]) =
    new DALProvider(classEntityInfo, rowTypeInfo, newKey, pos, dalApi, canBeProjected, canBeFullTextSearch, keyPolicy)

  override def execute[T](
      command: QueryCommand,
      projector: Either[FieldReader => Node[T], FieldReader => T],
      projKey: RelationKey[_],
      shapeType: TypeInfo[_],
      executeOptions: ExecuteOptions): ProviderRelation = {
    command.formattedQuery match {
      case ExpressionQuery(ex, QueryPlan.Accelerated) =>
        new accelerated.DALAccExecutionProvider(ex, projector, shapeType, projKey, dalApi, executeOptions)
      case ExpressionQuery(ex, QueryPlan.FullTextSearch) =>
        new fullTextSearch.FullTextSearchExecutionProvider(ex, projector, shapeType, projKey, dalApi, executeOptions)
      case ExpressionQuery(ex, plan @ (QueryPlan.Default | QueryPlan.Sampling)) =>
        new core.DALExecutionProvider(ex, plan, projector, shapeType, projKey, dalApi, executeOptions, pos)
    }
  }

  override def executeDeferred[T](
      command: QueryCommand,
      projector: Either[FieldReader => Node[T], FieldReader => T],
      projKey: RelationKey[Any],
      paramValues: List[Any],
      shapeType: TypeInfo[_],
      executeOptions: ExecuteOptions): ProviderRelation = {
    command.formattedQuery match {
      case ExpressionQuery(ex, QueryPlan.Accelerated) =>
        val newEx = ParameterReplacer.replace(command.parameters.zip(paramValues), ex)
        new accelerated.DALAccExecutionProvider(newEx, projector, shapeType, projKey, dalApi, executeOptions)
      case ExpressionQuery(ex, QueryPlan.FullTextSearch) =>
        val newEx = ParameterReplacer.replace(command.parameters.zip(paramValues), ex)
        new fullTextSearch.FullTextSearchExecutionProvider(newEx, projector, shapeType, projKey, dalApi, executeOptions)
      case ExpressionQuery(ex, plan @ (QueryPlan.Default | QueryPlan.Sampling)) =>
        val newEx = ParameterReplacer.replace(command.parameters.zip(paramValues), ex)
        new core.DALExecutionProvider(newEx, plan, projector, shapeType, projKey, dalApi, executeOptions, pos)
    }
  }

  override def toQueryBuilder: QueryBuilder[_] = {
    DALFrom(classEntityInfo.runtimeClass, keyPolicy, pos)
  }

  override def copyAsEntitledOnly() =
    new DALProvider(
      classEntityInfo,
      rowTypeInfo,
      key,
      pos,
      dalApi,
      canBeProjected,
      canBeFullTextSearch,
      keyPolicy,
      true)

  val supportsRegisteredIndexes: Boolean = {
    if (EvaluationContext.isInitialised) {
      Option(EvaluationContext.env)
        .flatMap(env => Option(env.entityResolver))
        .exists {
          case f: SessionFetcher =>
            f.dsi.serverFeatures().registeredIndexes.exists(_.supports(classEntityInfo.runtimeClass.getName))
          case _ => false
        }
    } else false
  }

  val isFullTextSearchEnabled: Boolean = PriqlSettings.enableFullTextSearchPriql
}

object DALProvider {
  import optimus.platform.relational.internal.OptimusCoreAPI.liftNode

  def apply[A <: Entity: TypeInfo](
      classEntityInfo: ClassEntityInfo,
      key: RelationKey[_],
      pos: MethodPosition,
      dalApi: DalAPI,
      keyPolicy: KeyPropagationPolicy): DALProvider = {
    val canBeProjected = classEntityInfo.isStorable && isProjectedEntity(classEntityInfo.runtimeClass)
    val canBeFullTextSearch = classEntityInfo.isStorable && isFullTextSearchEnabledEntity(classEntityInfo.runtimeClass)
    new DALProvider(
      classEntityInfo,
      implicitly[TypeInfo[A]],
      key,
      pos,
      dalApi,
      canBeProjected,
      canBeFullTextSearch,
      keyPolicy)
  }

  def unapply(dalProvider: DALProvider) = Some(dalProvider.classEntityInfo, dalProvider.key, dalProvider.pos)

  private[dal] val Payload = "dal$payload"
  private[dal] val VersionedRef = "dal$versionRef"
  private[dal] val EntityRef = "dal$entityRef"
  private[dal] val TemporalContext = "dal$temporalContext"
  private[dal] val StorageInfo = "dal$storageInfo"
  private[dal] val StorageTxTime = "dal$storageInfo.txTime"
  private[dal] val InitiatingEvent = "dal$initiatingEvent"
  private[dal] val ChildRef = "child_ref"
  private[dal] val ParentPropName = "parent_prop_name"
  private[dal] val ParentRef = "parent_ref"

  val EntityRefType = typeInfo[EntityReference]
  val VersionedRefType = typeInfo[VersionedReference]
  val InitiatingEventType = typeInfo[Option[BusinessEvent]]
  val TemporalContextType = typeInfo[TemporalContext]
  val PersistentEntityType = typeInfo[PersistentEntity]
  val EntityType = typeInfo[Entity]
  val StorageTxTimeType = typeInfo[Instant]

  private[dal] val getEntity: FieldReader => Any = { r: FieldReader =>
    r.readValue(0)(DALProvider.EntityType)
  }

  private[dal] val getEntityOption: FieldReader => Any = { r: FieldReader =>
    if (r.isNone(0)) None
    else Option(r.readValue(0)(DALProvider.EntityType))
  }

  val deserializeEntityReference: FieldReader => Node[Any] = liftNode { r: FieldReader =>
    val eRef = r.readValue(0)(DALProvider.EntityRefType)
    EvaluationContext.env.entityResolver.findByReference(eRef, r.pickledInputStream.temporalContext)
  }

  val deserializeEntityReferenceOption: FieldReader => Node[Any] = liftNode { r: FieldReader =>
    if (r.isNone(0)) None
    else {
      val eRef = r.readValue(0)(DALProvider.EntityRefType)
      Option(EvaluationContext.env.entityResolver.findByReference(eRef, r.pickledInputStream.temporalContext))
    }
  }

  val readFirstBooleanValue: FieldReader => Boolean = r => r.readBoolean(0)
  val readFirstIntValue: FieldReader => Int = r => r.readInt(0)
  val readFirstLongValue: FieldReader => Long = r => r.readLong(0)
  val readFirstDoubleValue: FieldReader => Double = r => r.readDouble(0)
  val readFirstFloatValue: FieldReader => Float = r => r.readFloat(0)
  val readFirstShortValue: FieldReader => Short = r => r.readShort(0)
  val readFirstCharValue: FieldReader => Char = r => r.readChar(0)
  val readFirstByteValue: FieldReader => Byte = r => r.readByte(0)
  val readFirstStringValue: FieldReader => String = r => r.readString(0)
  val readFirstAnyValue: FieldReader => Any = r => r.readValue(0)(TypeInfo.ANY)
  def readFirstAnyValueAndUnpickle(u: Unpickler[_]): FieldReader => Node[Any] = liftNode { r: FieldReader =>
    u.unpickle(r.readValue(0)(TypeInfo.ANY), r.pickledInputStream)
  }

  val readFirstBooleanValueOption: FieldReader => Option[Boolean] = r => r.readOptionBoolean(0)
  val readFirstIntValueOption: FieldReader => Option[Int] = r => r.readOptionInt(0)
  val readFirstLongValueOption: FieldReader => Option[Long] = r => r.readOptionLong(0)
  val readFirstDoubleValueOption: FieldReader => Option[Double] = r => r.readOptionDouble(0)
  val readFirstFloatValueOption: FieldReader => Option[Float] = r => r.readOptionFloat(0)
  val readFirstShortValueOption: FieldReader => Option[Short] = r => r.readOptionShort(0)
  val readFirstCharValueOption: FieldReader => Option[Char] = r => r.readOptionChar(0)
  val readFirstByteValueOption: FieldReader => Option[Byte] = r => r.readOptionByte(0)
  val readFirstStringValueOption: FieldReader => Option[String] = r => r.readOptionString(0)
  val readFirstAnyValueOption: FieldReader => Any = r => r.readOptionValue(0)(TypeInfo.ANY)
  def readFirstAnyValueAndUnpickleOption(u: Unpickler[_]): FieldReader => Node[Any] = liftNode { r: FieldReader =>
    u.unpickle(r.readOptionValue(0)(TypeInfo.ANY), r.pickledInputStream)
  }

  def knownProjectorToLambda(pj: RelationElement): Option[Either[FieldReader => Node[Any], FieldReader => Any]] = {
    pj match {
      case d: DALHeapEntityElement =>
        d.members.head.rowTypeInfo match {
          case PersistentEntityType =>
            Some(Right(getEntity))
          case EntityRefType =>
            Some(Left(deserializeEntityReference))
          case x =>
            throw new RelationalUnsupportedException(s"Unsupported shape type of DALHeapEntity.members[0]: $x")
        }
      case OptionElement(d: DALHeapEntityElement) =>
        d.members.head.rowTypeInfo match {
          case PersistentEntityType =>
            Some(Right(getEntityOption))
          case EntityRefType =>
            Some(Left(deserializeEntityReferenceOption))
          case x =>
            throw new RelationalUnsupportedException(s"Unsupported shape type of DALHeapEntity.members[0]: $x")
        }
      case OptionColumnElement(rowTypeInfo, unpicklerOpt) =>
        rowTypeInfo match {
          case TypeInfo.BOOLEAN => Some(Right(readFirstBooleanValueOption))
          case TypeInfo.INT     => Some(Right(readFirstIntValueOption))
          case TypeInfo.LONG    => Some(Right(readFirstLongValueOption))
          case TypeInfo.DOUBLE  => Some(Right(readFirstDoubleValueOption))
          case TypeInfo.FLOAT   => Some(Right(readFirstFloatValueOption))
          case TypeInfo.SHORT   => Some(Right(readFirstShortValueOption))
          case TypeInfo.CHAR    => Some(Right(readFirstCharValueOption))
          case TypeInfo.BYTE    => Some(Right(readFirstByteValueOption))
          case TypeInfo.STRING  => Some(Right(readFirstStringValueOption))
          case i =>
            unpicklerOpt.getOrElse(PicklerSelector.getUnpickler(i)) match {
              case _: IdentityUnpickler[_] => Some(Right(readFirstAnyValueOption))
              case u                       => Some(Left(readFirstAnyValueAndUnpickleOption(new OptionUnpickler(u))))
            }
        }
      case c: ColumnElement =>
        c.rowTypeInfo match {
          case TypeInfo.BOOLEAN => Some(Right(readFirstBooleanValue))
          case TypeInfo.INT     => Some(Right(readFirstIntValue))
          case TypeInfo.LONG    => Some(Right(readFirstLongValue))
          case TypeInfo.DOUBLE  => Some(Right(readFirstDoubleValue))
          case TypeInfo.FLOAT   => Some(Right(readFirstFloatValue))
          case TypeInfo.SHORT   => Some(Right(readFirstShortValue))
          case TypeInfo.CHAR    => Some(Right(readFirstCharValue))
          case TypeInfo.BYTE    => Some(Right(readFirstByteValue))
          case TypeInfo.STRING  => Some(Right(readFirstStringValue))
          case i =>
            c.columnInfo.unpickler.getOrElse(PicklerSelector.getUnpickler(i)) match {
              case _: IdentityUnpickler[_] => Some(Right(readFirstAnyValue))
              case u                       => Some(Left(readFirstAnyValueAndUnpickle(u)))
            }
        }
      case _ => None
    }
  }

  def readEntityReferenceHolder[T <: Entity: TypeInfo]: FieldReader => EntityReferenceHolder[T] = r => {
    val eRef = r.readValue(0)(DALProvider.EntityRefType)
    val txTime = r.readOptionValue(1)(TypeInfo.ANY).collect { case buf: Seq[_] =>
      val seconds = buf(0).asInstanceOf[Number].longValue
      val nanos = buf(1).asInstanceOf[Int]
      Instant.ofEpochSecond(seconds, nanos)
    }
    EntityReferenceHolder
      .withKnownAbstractType(eRef, r.pickledInputStream.temporalContext, txTime, None, typeInfo[T].clazz)
  }

  def readEntityReferenceHolderWithVref[T <: Entity: TypeInfo]: FieldReader => EntityReferenceHolder[T] = r => {
    val eRef = r.readValue(0)(DALProvider.EntityRefType)
    val txTime = r.readOptionValue(1)(TypeInfo.ANY).collect { case buf: Seq[_] =>
      val seconds = buf(0).asInstanceOf[Number].longValue
      val nanos = buf(1).asInstanceOf[Int]
      Instant.ofEpochSecond(seconds, nanos)
    }
    val versionedRef = r.readOptionValue(2)(DALProvider.VersionedRefType)
    EntityReferenceHolder
      .withKnownAbstractType(eRef, r.pickledInputStream.temporalContext, txTime, versionedRef, typeInfo[T].clazz)
  }

  def isProjectedEntity(runtimeClass: Class[_]): Boolean = {
    runtimeClass.getAnnotation(classOf[EntityMetaDataAnnotation]).projected && !PriqlSettings.projectedEntityDenyList
      .contains(runtimeClass.getName)
  }

  def isProjectedEmbeddable(runtimeClass: Class[_]): Boolean = {
    runtimeClass.getAnnotation(classOf[EmbeddableMetaDataAnnotation]).projected
  }

  def isFullTextSearchEnabledEntity(runtimeClass: Class[_]): Boolean = {
    runtimeClass.getAnnotation(classOf[EntityMetaDataAnnotation]).fullTextSearch
  }
}

private object OptionColumnElement {
  def unapply(e: RelationElement): Option[(TypeInfo[_], Option[Unpickler[_]])] = {
    e match {
      case OptionElement(c: ColumnElement) =>
        Some(c.rowTypeInfo -> c.columnInfo.unpickler)
      case c: ColumnElement if c.rowTypeInfo <:< classOf[Option[_]] =>
        Some(c.rowTypeInfo.typeParams.head -> None)
      case _ => None
    }
  }
}
