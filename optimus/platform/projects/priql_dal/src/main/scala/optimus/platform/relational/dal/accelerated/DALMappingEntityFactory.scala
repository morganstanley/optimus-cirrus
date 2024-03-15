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
package optimus.platform.relational.dal.accelerated

import optimus.entity.ClassEntityInfo
import optimus.entity.EntityInfoRegistry
import optimus.graph.NodeTaskInfo
import optimus.graph.PropertyInfo
import optimus.graph.TweakTarget
import optimus.platform.EvaluationContext
import optimus.platform.ScenarioStack
import optimus.platform.annotations.internal.AccelerateInfoAnnotation
import optimus.platform.annotations.internal._projected
import optimus.platform.dsi.expressions.Embeddable
import optimus.platform.dsi.expressions.Expression
import optimus.platform.dsi.expressions.Id
import optimus.platform.dsi.expressions.{Entity => EntityExpression}
import optimus.platform.dsi.expressions.{Linkage => LinkageExpression}
import optimus.platform.pickling.Registry
import optimus.platform.pickling.Unpickler
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.dal.core.DALMappingEntity
import optimus.platform.relational.data.mapping.MemberInfo
import optimus.platform.relational.data.mapping.QueryBinder
import optimus.platform.relational.data.tree.ColumnInfo
import optimus.platform.relational.data.tree.ColumnType
import optimus.platform.relational.internal.RelationalUtils
import optimus.platform.relational.tree.ConstructorDescriptor
import optimus.platform.relational.tree.FieldDescriptor
import optimus.platform.relational.tree.MethodDescriptor
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.storable.EmbeddableCompanionBase
import optimus.platform.storable.Entity
import optimus.platform.util.ReflectUtils
import optimus.platform.versioning.TransformerRegistry
import optimus.platform.versioning.TransformerRegistry.ClientVertex

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.reflect.runtime.{currentMirror => cm}
import optimus.scalacompat.collection._

import scala.collection.compat._

object DALMappingEntityFactory {
  def entity(info: ClassEntityInfo, projectedType: TypeInfo[_], tableId: String): DALMappingEntity = {
    DALEntity(info, projectedType, tableId)
  }

  def linkage(tableId: String, childType: TypeInfo[_]): DALMappingEntity = {
    DALLinkage(tableId, childType)
  }

  def embeddable(owner: TypeInfo[_], property: String, embeddableType: TypeInfo[_]): DALMappingEntity = {
    DALEmbeddable(owner, property, embeddableType)
  }

  private def getAddedFieldMap(className: String): Map[String, Any] = {
    // it is the same with StorableSerializer.version method, we only consider canonical type's version
    val canonicalShape = TransformerRegistry.getCanonicalShape(className)
    if (canonicalShape.isDefined) {
      val fieldMap = new mutable.HashMap[String, Any]
      val vv1 = ClientVertex(canonicalShape.get)
      getAllAddedFieldMap(vv1).foreach(_.foreach { case (field, value) =>
        fieldMap
          .get(field)
          .map(v =>
            if (v != value)
              throw new RelationalUnsupportedException(
                s"added $field has different default value for safe transformer shape $className")
            else v)
          .getOrElse(fieldMap.put(field, value))
      })
      fieldMap.toMap[String, Any]
    } else Map.empty[String, Any]
  }

  private def getAllAddedFieldMap(startPoint: ClientVertex): Seq[Map[String, Any]] = {
    val clientGraph = TransformerRegistry.getClientGraph
    clientGraph
      .getAllInboundEdges(startPoint)
      .iterator
      .map(edge => {
        if (!edge.isSafe)
          throw new RelationalUnsupportedException(
            s"there is non-safe transformer definition from ${edge.source.data} to ${startPoint.data}")
        if (edge.hasRename)
          throw new RelationalUnsupportedException(
            s"there is rename defined in transformer from ${edge.source.data} to ${startPoint.data}")
        edge.addedFieldMap
      })
      .toIndexedSeq
  }

  final case class DALEmbeddable(owner: TypeInfo[_], property: String, projectedType: TypeInfo[_])
      extends DALMappingEntity {
    val tableId = s"${owner.runtimeClassName}_${property}[${projectedType.clazz.getSimpleName}]"
    private val companion = ReflectUtils.getCompanion[EmbeddableCompanionBase](projectedType.clazz)
    private def unpickler = Some(Registry.unsafeUnpicklerOfClass(projectedType.clazz.asInstanceOf[Class[Embeddable]]))

    lazy val addedFieldMap = getAddedFieldMap(projectedType.clazz.getName)

    val (mappedMembers, replacedMembers) = {
      val memberList = new mutable.ListBuffer[MemberInfo]
      val replaced = new mutable.HashMap[String, TypeInfo[_]]

      memberList += MemberInfo(projectedType, DALProvider.VersionedRef, DALProvider.VersionedRefType, None)
      memberList += MemberInfo(projectedType, DALProvider.Payload, projectedType, unpickler)
      companion.projectedMembers.foreach {
        case f: FieldDescriptor =>
          val queryable = f.metadata("queryable").asInstanceOf[Boolean]
          if (queryable) {
            val fieldUnpickler = f.metadata.get("unpickler").asInstanceOf[Option[Unpickler[_]]]
            memberList += getMember(projectedType, f.name, f.fieldType, fieldUnpickler, replaced)
          }
        case _ =>
      }
      (memberList.result(), replaced.toMap)
    }
    val mappedMemberLookup: HashMap[String, MemberInfo] =
      mappedMembers.iterator.map(t => t.name -> t).convertTo(HashMap)

    def isColumn(member: MemberInfo): Boolean = {
      mappedMemberLookup
        .get(member.name)
        .map(mi => QueryBinder.memberTypeEquals(mi.memberType, member.memberType))
        .getOrElse(false)
    }
    def isMapped(member: String): Boolean = mappedMemberLookup.contains(member)
    def getMappedMember(member: String): MemberInfo = mappedMemberLookup(member)
    def getColumnInfo(member: MemberInfo): ColumnInfo = {
      if (member.name eq DALProvider.VersionedRef) ColumnInfo(ColumnType.EntityRef, member.unpickler)
      else {
        val unpickler = member.unpickler.map(PicklerSelector.getInnerUnpickler)
        ColumnInfo(ColumnType.Default, unpickler)
      }
    }
    def getCompoundMembers(member: String): Option[List[MemberInfo]] = None
    def getDefaultValue(member: String): Option[Any] = addedFieldMap.get(member)
    def getOriginalType(member: String): TypeInfo[_] = {
      replacedMembers.get(member).getOrElse(mappedMemberLookup(member).memberType)
    }
    def format(id: Id): Expression = {
      val tag = {
        val classSymbol = cm.classSymbol(projectedType.clazz)
        classSymbol.name.toString
      }
      Embeddable(owner.clazz.getName, property, tag, id)
    }
  }

  final case class DALLinkage(tableId: String, childType: TypeInfo[_]) extends DALMappingEntity {
    def projectedType: TypeInfo[_] = TypeInfo.NOTHING
    val (mappedMembers, replacedMembers) = {
      val memberList = new mutable.ListBuffer[MemberInfo]
      val replaced = new mutable.HashMap[String, TypeInfo[_]]

      memberList += getMember(projectedType, DALProvider.ChildRef, childType, None, replaced)
      memberList += MemberInfo(projectedType, DALProvider.ParentPropName, TypeInfo.STRING, None)
      memberList += MemberInfo(projectedType, DALProvider.ParentRef, DALProvider.EntityRefType, None)

      (memberList.result(), replaced.toMap)
    }

    val mappedMemberLookup: HashMap[String, MemberInfo] =
      mappedMembers.iterator.map(t => t.name -> t).convertTo(HashMap)

    def isColumn(member: MemberInfo): Boolean = {
      mappedMemberLookup
        .get(member.name)
        .map(mi => QueryBinder.memberTypeEquals(mi.memberType, member.memberType))
        .getOrElse(false)
    }
    def isMapped(member: String): Boolean = mappedMemberLookup.contains(member)
    def getMappedMember(member: String): MemberInfo = mappedMemberLookup(member)
    def getColumnInfo(member: MemberInfo): ColumnInfo = {
      if (member.memberType == TypeInfo.STRING)
        ColumnInfo(ColumnType.Default, member.unpickler)
      else
        ColumnInfo(ColumnType.EntityRef, member.unpickler)
    }
    def getCompoundMembers(member: String): Option[List[MemberInfo]] = None
    def getDefaultValue(member: String): Option[Any] = None
    def getOriginalType(member: String): TypeInfo[_] = {
      replacedMembers.get(member).getOrElse(mappedMemberLookup(member).memberType)
    }
    def format(id: Id): Expression = LinkageExpression(tableId, null, id)
  }

  final case class DALEntity(entityInfo: ClassEntityInfo, projectedType: TypeInfo[_], tableId: String)
      extends DALMappingEntity {
    lazy val addedFieldMap = getAddedFieldMap(entityInfo.runtimeClass.getName)

    val (mappedMembers, indirectMembers, replacedMembers) = {
      val propertyMap = entityInfo.storedProperties
        .map(p => (p.name, (p.typeInfo, p.unpickler)))
        .toMap[String, (TypeInfo[_], Unpickler[_])]
      val memberList = new mutable.ListBuffer[MemberInfo]
      val indirectMemberList = new mutable.HashMap[String, List[MemberInfo]]
      val replaced = new mutable.HashMap[String, TypeInfo[_]]
      val entityMethods = RelationalUtils.getMethodsMap(entityInfo.runtimeClass)

      // We should always put dal$entityRef as the first member of an entity.
      memberList += MemberInfo(projectedType, DALProvider.EntityRef, DALProvider.EntityRefType, None)
      memberList += MemberInfo(projectedType, DALProvider.StorageTxTime, DALProvider.StorageTxTimeType)
      memberList += MemberInfo(projectedType, DALProvider.VersionedRef, DALProvider.VersionedRefType)
      memberList += MemberInfo(projectedType, DALProvider.InitiatingEvent, DALProvider.InitiatingEventType)
      for (indexInfo <- entityInfo.indexes) {
        val accAnno =
          RelationalUtils.getMethodAnnotation(entityMethods, indexInfo.name, classOf[_projected]).getOrElse(null)
        if (accAnno != null && accAnno.queryable()) {
          if (indexInfo.propertyNames.size > 1) {
            // here we cannot use getMember, otherwise bindMember won't find this compound member
            val members = indexInfo.propertyNames
              .map(name => {
                val (memberType, unpickler) = propertyMap(name)
                MemberInfo(projectedType, name, memberType, Some(unpickler))
              })
              .toList
            indirectMemberList.put(indexInfo.name, members)
            // we assume index will never reference tweak-able property
            memberList += getMember(
              projectedType,
              indexInfo.name,
              TypeInfo.mkTuple(members.map(_.memberType)),
              None,
              replaced)
          } else
            propertyMap.get(indexInfo.propertyNames.head) foreach { case (memberType, unpickler) =>
              if (indexInfo.propertyNames.head != indexInfo.name)
                indirectMemberList.put(
                  indexInfo.name,
                  getMember(projectedType, indexInfo.propertyNames.head, memberType, Some(unpickler), replaced) :: Nil)
              memberList += getMember(projectedType, indexInfo.name, memberType, Some(unpickler), replaced)
            }
        }
      }
      for (memberDesc <- entityInfo.extraProjectedMembers) {
        // we check AccelerateInfoAnnotation instead here
        val annon =
          RelationalUtils
            .getMethodAnnotation(entityMethods, memberDesc.name, classOf[AccelerateInfoAnnotation])
            .getOrElse(null)
        val accAnno =
          RelationalUtils.getMethodAnnotation(entityMethods, memberDesc.name, classOf[_projected]).getOrElse(null)
        if (annon != null && !annon.path.isEmpty && accAnno.queryable()) {
          val returnType = memberDesc match {
            case f: FieldDescriptor       => f.fieldType
            case m: MethodDescriptor      => m.returnType
            case c: ConstructorDescriptor => c.declaringType
          }
          val unpickler = memberDesc.metadata.get("unpickler").asInstanceOf[Option[Unpickler[_]]]
          indirectMemberList.put(
            memberDesc.name,
            getMember(projectedType, annon.path, returnType, unpickler, replaced) :: Nil)
          memberList += getMember(projectedType, memberDesc.name, returnType, unpickler, replaced)
        }
      }
      var resolver: TweakLookup = null // optimize the case when there is no tweak-able property
      val isTSStack = EvaluationContext.scenarioStack.isTrackingOrRecordingTweakUsage

      for (property <- entityInfo.storedProperties) {
        val isIndex = property.snapFlags() & NodeTaskInfo.INDEXED
        val isKey = property.snapFlags() & NodeTaskInfo.KEY
        if (isIndex == 0 && isKey == 0) {
          val anno =
            RelationalUtils.getMethodAnnotation(entityMethods, property.name, classOf[_projected]).getOrElse(null)
          if (anno != null && anno.queryable()) {
            if (property.isTweakable) {
              if (!isTSStack) {
                if (resolver eq null)
                  resolver = new TweakLookup(EvaluationContext.scenarioStack)
                if (!resolver.containsTweak(property)) {
                  val (memberType, unpickler) = propertyMap(property.name)
                  memberList += getMember(projectedType, property.name, memberType, Some(unpickler), replaced)
                }
              }
            } else {
              val (memberType, unpickler) = propertyMap(property.name)
              memberList += getMember(projectedType, property.name, memberType, Some(unpickler), replaced)
            }
          }
        }
      }
      (memberList.result(), indirectMemberList.toMap, replaced.toMap)
    }

    val mappedMemberLookup: HashMap[String, MemberInfo] =
      mappedMembers.iterator.map(t => t.name -> t).convertTo(HashMap)
    val columnMemberLookup: HashMap[String, MemberInfo] = {
      mappedMembers.iterator
        .filter(m => {
          // check childToParent flag
          !entityInfo.properties.exists(p => p.isChildToParent && p.name == m.name)
        })
        .map(t => t.name -> t)
        .convertTo(HashMap)
    }

    def isColumn(member: MemberInfo): Boolean = {
      columnMemberLookup
        .get(member.name)
        .map(mi => QueryBinder.memberTypeEquals(mi.memberType, member.memberType))
        .getOrElse(false)
    }
    def isMapped(member: String): Boolean = mappedMemberLookup.contains(member)
    def getMappedMember(member: String): MemberInfo = mappedMemberLookup(member)
    def getColumnInfo(member: MemberInfo): ColumnInfo = {
      val unpickler = member.unpickler.map(PicklerSelector.getInnerUnpickler)
      if (member.name eq DALProvider.EntityRef) ColumnInfo(ColumnType.EntityRef, unpickler)
      else {
        // val DALMappingEntity(info, _, _) = entity
        entityInfo.indexes
          .find(i => i.name == member.name)
          .map { i =>
            if (i.unique) {
              if (i.indexed) ColumnInfo(ColumnType.UniqueIndex, unpickler)
              else ColumnInfo(ColumnType.Key, unpickler)
            } else ColumnInfo(ColumnType.Index, unpickler)
          }
          .getOrElse(ColumnInfo(ColumnType.Default, unpickler))
      }
    }
    def getCompoundMembers(member: String): Option[List[MemberInfo]] = indirectMembers.get(member)
    def getDefaultValue(member: String): Option[Any] = addedFieldMap.get(member)
    def getOriginalType(member: String): TypeInfo[_] = {
      replacedMembers.get(member).getOrElse(mappedMemberLookup(member).memberType)
    }
    def format(id: Id): Expression = {
      val types: Seq[String] = entityInfo.baseTypes.iterator.map { _.runtimeClass.getName }.toIndexedSeq
      EntityExpression(tableId, null, types, id)
    }
  }

  private def getMember(
      shapeType: TypeInfo[_],
      name: String,
      originType: TypeInfo[_],
      unpickler: Option[Unpickler[_]],
      replaced: mutable.HashMap[String, TypeInfo[_]]): MemberInfo = {
    val underlyingType = TypeInfo.underlying(originType)
    if (underlyingType <:< classOf[Entity]) {
      val targetInfo = EntityInfoRegistry.getClassInfo(underlyingType.clazz)
      val canBeProjected = DALProvider.isProjectedEntity(targetInfo.runtimeClass)
      if (!canBeProjected) MemberInfo(shapeType, name, originType, unpickler)
      else {
        replaced.put(name, originType)
        val memberType = TypeInfo.replaceUnderlying(originType, DALProvider.EntityRefType)
        MemberInfo(shapeType, name, memberType, None)
      }
    } else MemberInfo(shapeType, name, originType, unpickler)
  }
}

private class TweakLookup(ss: ScenarioStack) {
  import TweakLookup._

  private val propertyTweaks = propertyTweakTargetTaskInfos(ss)
  private val instanceTweaks = instanceTweakTargetTaskInfos(ss)

  def containsTweak(info: PropertyInfo[_]): Boolean = {
    if (propertyTweaks.nonEmpty) {
      if (propertyTweaks.contains(info))
        return true
      var matchOn = info.matchOn()
      if (
        (matchOn ne null) && matchOn.exists(t =>
          propertyTweaks.contains(t.asInstanceOf[TweakTarget[_, _]].propertyInfo))
      )
        return true
      matchOn = info.matchOnChildren()
      if (
        (matchOn ne null) && matchOn.exists(t =>
          propertyTweaks.contains(t.asInstanceOf[TweakTarget[_, _]].propertyInfo))
      )
        return true
    }
    if (instanceTweaks.nonEmpty) {
      if (instanceTweaks.contains(info))
        return true
      val matchOn = info.matchOnChildren()
      if (
        (matchOn ne null) && matchOn.exists(t =>
          instanceTweaks.contains(t.asInstanceOf[TweakTarget[_, _]].propertyInfo))
      )
        return true
    }
    false
  }
}

private object TweakLookup {

  /**
   * Returns the related PropertyInfos touched in the property-tweak-targets
   */
  private def propertyTweakTargetTaskInfos(s: ScenarioStack): Set[NodeTaskInfo] = {
    val l = new mutable.ListBuffer[NodeTaskInfo]
    var ss = s
    while (!ss.isRoot) {
      l ++= ss._cacheID.allPropertyTweaksInfos
      ss = ss.parent
    }
    l.toSet
  }

  /**
   * Returns the related PropertyInfos touched in the instance-tweak-targets
   */
  private def instanceTweakTargetTaskInfos(s: ScenarioStack): Set[NodeTaskInfo] = {
    val l = new mutable.ListBuffer[NodeTaskInfo]
    var ss = s
    while (!ss.isRoot) {
      l ++= ss._cacheID.allInstanceTweaksInfos
      ss = ss.parent
    }
    l.toSet
  }
}
