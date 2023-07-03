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
package optimus.platform

import optimus.entity.StorableInfo
import optimus.platform._
import optimus.platform.AsyncImplicits._
import optimus.platform.relational.internal.{ReflectPropertyUtils, RelationalUtils}
import optimus.platform.relational.tree.{AnonymousField, TypeInfo}
import optimus.platform.relational.{RelationalException, tree}
import optimus.platform.storable._

// sometimes we might need to just use strings to identify key fields (esp. for backward compatibility)
final class DynamicKey private (val fields: Seq[String], val upperBound: TypeInfo[_]) extends RelationKey[Any] {
  private val nodeOrFunc: Either[NodeFunction1[Any, Seq[Any]], Function1[Any, Seq[Any]]] =
    if (
      upperBound <:< classOf[DynamicObject] || upperBound =:= TypeInfo.ANY ||
      upperBound.propertyNames.intersect(fields).size != fields.size
    ) {
      Left(asNode { (t: Any) =>
        for (p <- fields)
          yield t match {
            case d: DynamicObject => d.get(p)
            case _                => if (t == null) null else t.getClass.getMethod(p).invoke(t)
          }
      })
    } else {
      val fs = fields.map(f => upperBound.nodeProperties.getOrElse(f, new AnonymousField(f)))
      if (fs.forall(_.isSyncSafe))
        Right((t: Any) => fs.map(f => if (t == null) null else f.invokeOnSync(t)))
      else
        Left(asNode { (t: Any) =>
          fs.apar.map(f =>
            if (t == null) null
            else if (f.isSyncSafe) f.invokeOnSync(t)
            else f.invokeOn(t))
        })
    }

  @node override def of(t: Any): Seq[Any] = {
    nodeOrFunc match {
      case Left(nf) => nf(t)
      case Right(f) => f(t)
    }
  }

  override val isSyncSafe: Boolean = nodeOrFunc.isRight

  override def ofSync(t: Any): Seq[Any] = {
    nodeOrFunc.right.get.apply(t)
  }

  override def equals(o: Any): Boolean = {
    o match {
      case d: DynamicKey => d.fields == fields && d.upperBound =:= upperBound
      case _             => false
    }
  }

  override def hashCode: Int = fields.hashCode

  override lazy val isKeyComparable: Boolean = false

  override def compareKeys(leftKey: Any, rightKey: Any): Int = {
    throw new RelationalException("DynamicKey is not comparable")
  }
}

object DynamicKey {
  val ttag: TypeInfo[DynamicKey] = tree.typeInfo[DynamicKey]

  def apply(fields: Seq[String], upperBound: TypeInfo[_] = TypeInfo.ANY): DynamicKey =
    new DynamicKey(fields, upperBound)
}

/**
 * EntityKey is used with Optimus entities and simply uses the declared key of the underlying entity class to act as the
 * identifying property for the entity type.
 */
class EntityKey[-T <: Storable: TypeInfo] private (val info: StorableInfo) extends RelationKey[T] {
  @node override def of(t: T): AnyRef = {
    if ((t eq null) || !t.dal$isTemporary || !defaultKey.isDefined)
      t
    else
      defaultKey.get.getRaw(t).asInstanceOf[AnyRef]
  }

  val defaultKey = RelationalUtils.extractEntityDefaultKey(info)

  val fields = {
    defaultKey.map(k => k.propertyNames).getOrElse(RelationalUtils.extractConstructorValNames(tree.typeInfo[T]))
  }

  override def equals(o: Any): Boolean = o match {
    case d: EntityKey[_] => d.info == info
    case _               => false
  }

  override def hashCode: Int = info.hashCode

  override lazy val isKeyComparable: Boolean = RelationKey.isFieldsComparable(implicitly[TypeInfo[T]], fields)

  override def compareKeys(leftKey: Any, rightKey: Any): Int = {
    (leftKey, rightKey) match {
      case (null, null) => 0
      case (null, _)    => -1
      case (_, null)    => 1
      case (l: Any, r: Any) =>
        if (defaultKey.isDefined)
          RelationKey.compareTwoValues(RelationalUtils.box(l), RelationalUtils.box(r))
        else if (leftKey.asInstanceOf[Entity].dal$isTemporary && rightKey.asInstanceOf[Entity].dal$isTemporary)
          fullArgsCompare(l, r)
        else entityRefCompare(leftKey.asInstanceOf[Entity], rightKey.asInstanceOf[Entity])
    }
  }

  private def fullArgsCompare(l: Any, r: Any): Int = {
    // c is var because we need assign it when comparing the keys.
    var c = 0
    val it = fields.iterator
    while (c == 0 && it.hasNext) {
      val f = it.next()
      c = RelationKey.compareTwoValues(
        ReflectPropertyUtils.getPropertyValue(f, l.getClass(), l),
        ReflectPropertyUtils.getPropertyValue(f, r.getClass(), r))
    }
    c
  }

  private def entityRefCompare(l: Entity, r: Entity): Int = {
    RelationKey.compareArrayData(l.dal$entityRef.data, r.dal$entityRef.data)
  }
}

object EntityKey {
  def apply[T <: Storable: TypeInfo](info: StorableInfo): EntityKey[T] = new EntityKey[T](info)
  def apply[T <: Entity: TypeInfo](entityBase: EntityCompanionBase[T]): EntityKey[T] = new EntityKey[T](entityBase.info)

  def typeInfo[T <: Storable: TypeInfo] = tree.typeInfo[EntityKey[T]]
}

/**
 * TupleKey is produced through the combining of two underlying keys and removing duplicate properties.
 *
 * The reason we include K1 and K2 in the type parameters is so that the behavior of the TupleKey is fully captured by
 * the type
 */
class TupleKey[T1, T2, K1 <: RelationKey[T1], K2 <: RelationKey[T2]] private (val k1: K1, val k2: K2)
    extends RelationKey[(T1, T2)] {
  @node override def of(t: (T1, T2)): AnyRef = (k1.of(t._1), k2.of(t._2))

  def fields: Seq[String] = (k1.fields ++ k2.fields).toSet.toSeq

  override lazy val isKeyComparable: Boolean = k1.isKeyComparable && k2.isKeyComparable

  override def compareKeys(leftKey: Any, rightKey: Any): Int = {
    (leftKey, rightKey) match {
      case (null, null) => 0
      case (null, _)    => -1
      case (_, null)    => 1
      case ((l1, l2), (r1, r2)) =>
        val c = k1.compareKeys(l1, r1)
        if (c == 0) k2.compareKeys(l2, r2)
        else c
      case _ => throw new RelationalException("input key type should be Tuple2[_, _] for TupleKey compare")
    }
  }
}

object TupleKey {
  def apply[T1, T2, K1 <: RelationKey[T1], K2 <: RelationKey[T2]](k1: K1, k2: K2) = new TupleKey[T1, T2, K1, K2](k1, k2)
}

/**
 * converts a static key to works as dynamic key based on the same fields
 *
 * TODO (OPTIMUS-11569): should have separate classes for the different cases and a factory method or a method to
 * convert any key to a dynamic key
 */
class DynamicFromStaticKey(val sk: RelationKey[_]) extends RelationKey[DynamicObject] {
  @node override def of(t: DynamicObject): AnyRef = ofSync(t).asInstanceOf[AnyRef]
  override def isSyncSafe = true
  override def ofSync(t: DynamicObject): Any = sk match {
    case s: SuperKey[_] => t.getAll
    case _              => fields.map(t.get)
  }
  def fields: Seq[String] = sk.fields
  override lazy val isKeyComparable: Boolean = false
}

class DynamicExtendedKey(val sk: RelationKey[_], val extraField: String) extends RelationKey[DynamicObject] {
  @node override def of(t: DynamicObject): AnyRef = ofSync(t).asInstanceOf[AnyRef]
  override def isSyncSafe = true
  override def ofSync(t: DynamicObject): Any = fields.map(t.get)

  def fields: Seq[String] = extraField +: sk.fields
  override val isKeyComparable: Boolean = false
}

/**
 * CompositeKey is produced through the combining of two underlying keys.
 */
// the reason we include K1 and K2 in the type parameters is so that the behavior of the CompositeKey is fully
// captured by the type
class CompositeKey[T1, T2, K1 <: RelationKey[T1], K2 <: RelationKey[T2]] private (val k1: K1, val k2: K2)
    extends RelationKey[T1 with T2] {
  @node override def of(t: T1 with T2): AnyRef = (k1.of(t), k2.of(t))

  def fields: Seq[String] = k1.fields ++ k2.fields

  override def equals(o: Any): Boolean = o match {
    case d: CompositeKey[_, _, _, _] => d.k1 == k1 && d.k2 == k2
    case _                           => false
  }

  override def hashCode: Int = k1.hashCode + 37 * k2.hashCode

  override lazy val isKeyComparable: Boolean = {
    k1.isKeyComparable && k2.isKeyComparable
  }

  override def compareKeys(leftKey: Any, rightKey: Any): Int = {
    (leftKey, rightKey) match {
      case (null, null) => 0
      case (null, _)    => -1
      case (_, null)    => 1
      case ((l1, l2), (r1, r2)) =>
        val c = k1.compareKeys(l1, r1)
        if (c == 0) k2.compareKeys(l2, r2)
        else c
      case _ => throw new RelationalException("input key type should be Tuple2[_, _] for CompositeKey compare")
    }
  }
}

object CompositeKey {
  def apply[T1, T2, K1 <: RelationKey[T1], K2 <: RelationKey[T2]](k1: K1, k2: K2) =
    new CompositeKey[T1, T2, K1, K2](k1, k2)
}

// represents a key applied to the left entry of a tuple (as the result of groupBy)
class LeftKey[GK, GV, K <: RelationKey[GK]](k: K) extends RelationKey[(GK, GV)] {
  @node override def of(t: (GK, GV)): AnyRef = k.of(t._1)

  def fields: Seq[String] = k.fields.map("_1." + _)

  override lazy val isKeyComparable: Boolean = k.isKeyComparable

  override def compareKeys(leftKey: Any, rightKey: Any): Int = {
    (leftKey, rightKey) match {
      case (null, null) => 0
      case (null, _)    => -1
      case (_, null)    => 1
      case (l, r)       => k.compareKeys(l, r)
    }
  }
}

object LeftKey {
  def apply[GK, GV, K <: RelationKey[GK]](k: K) = new LeftKey[GK, GV, K](k)
}

object NoKey extends RelationKey[Any] {
  @node override def of(t: Any): AnyRef = ???

  def fields: Seq[String] = Nil

  override lazy val isKeyComparable: Boolean = false
}
