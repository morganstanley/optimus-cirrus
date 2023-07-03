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
package optimus.platform.relational

import optimus.entity.EntityInfoRegistry
import optimus.platform.relational.internal.RelationalTreeUtils
import optimus.platform._
import optimus.platform.{NoKey => NK, _}
import optimus.platform.relational.tree.{TypeInfo, typeInfo}
import optimus.platform.storable.Entity
import optimus.platform.storable.Storable

/**
 * This trait defines the rules to propagate relation key, used as a mixin for DefaultQueryProvider
 */
trait KeyPropagationPolicy {

  /**
   * Make a relation key based on the supplied type info
   */
  def mkKey[T: TypeInfo]: RelationKey[T]

  /**
   * Relation key propagation rule for projection operators
   */
  def mkProject[From: TypeInfo, To: TypeInfo](key: RelationKey[From]): RelationKey[To]

  /**
   * Relation key propagation rule for join, etc
   */
  def mkTuple[From1: TypeInfo, From2: TypeInfo](
      key1: RelationKey[From1],
      key2: RelationKey[From2]): RelationKey[(From1, From2)]

  /**
   * Relation key propagation rule for grouping, etc
   */
  def mkPartialTuple[From1: TypeInfo, From2: TypeInfo](
      key: Either[RelationKey[From1], RelationKey[From2]]): RelationKey[(From1, From2)]

  /**
   * Relation key propagation rule for extendTyped, etc
   */
  def mkComposite[From: TypeInfo, Extend: TypeInfo](key: RelationKey[From])(implicit
      compositeType: TypeInfo[From with Extend]): RelationKey[From with Extend]

  /**
   * Relation key propagation rule for onNatural
   */
  def mkFlatten[Left: TypeInfo, Right: TypeInfo](key: RelationKey[(Left, Right)])(implicit
      flattenType: TypeInfo[Left with Right]): RelationKey[Left with Right]

  /**
   * Relation key propagation rule for union/difference, returns result type and result key
   */
  def mkUnion(types: Iterable[TypeInfo[_]], keys: Iterable[RelationKey[_]]): (TypeInfo[_], RelationKey[_])

  /**
   * Relation key propagation rule for withLeftDefault, withRightDefault
   */
  def mkUpcast[From: TypeInfo, To >: From: TypeInfo](key: RelationKey[From]): RelationKey[To]

  /**
   * Relation key propagation rule for untype, extend, etc
   */
  def mkDynamic[From: TypeInfo](key: RelationKey[From], field: String): RelationKey[DynamicObject]

  /**
   * Relation key propagation rule for aggregateUntyped
   */
  def mkDynamic(fields: Seq[String], upperBound: TypeInfo[_]): RelationKey[Any]
}

object KeyPropagationPolicy {
  trait Legacy extends KeyPropagationPolicy {
    def mkKey[T: TypeInfo]: RelationKey[T] = {
      val itemType = typeInfo[T]
      if (itemType <:< classOf[Entity]) {
        // if the shape is an entity, we can use an EntityKey
        val info = EntityInfoRegistry.getInfo(itemType.clazz)
        EntityKey[Storable](info)(itemType.cast[Storable]).asInstanceOf[RelationKey[T]]
      } else if (itemType <:< classOf[DynamicObject]) {
        NK
      } else {
        // fallback -- we can always use a super key
        SuperKey[T]
      }
    }

    def mkProject[From: TypeInfo, To: TypeInfo](key: RelationKey[From]): RelationKey[To] = {
      val fromType = typeInfo[From]
      val toType = typeInfo[To]

      // rule 1: if the shape type is unchanged, we can keep the same key
      if (fromType =:= toType) key.asInstanceOf[RelationKey[To]]
      else
        key match {
          // rule 2: if original key was a modulo key, and the modulo type is a super-type of the new shape,
          // we can keep using the same modulo key (but with the shape changed). this is really useful for calc Results.
          case mk: ModuloKey[_, _] if toType <:< mk.moduloTypeInfo => ModuloKey(toType, mk.moduloTypeInfo.cast[Any])

          // rule 3: fall back to rules in mkKey method
          case _ => mkKey[To]
        }
    }

    def mkTuple[From1: TypeInfo, From2: TypeInfo](
        key1: RelationKey[From1],
        key2: RelationKey[From2]): RelationKey[(From1, From2)] = {
      TupleKey(key1, key2)
    }

    def mkPartialTuple[From1: TypeInfo, From2: TypeInfo](
        key: Either[RelationKey[From1], RelationKey[From2]]): RelationKey[(From1, From2)] = {
      key match {
        case Left(NK)  => NK
        case Left(key) => LeftKey[From1, From2, RelationKey[From1]](key)
        case _         => throw new RelationalException("Not implemented!")
      }
    }

    def mkComposite[From: TypeInfo, Extend: TypeInfo](key: RelationKey[From])(implicit
        compositeType: TypeInfo[From with Extend]): RelationKey[From with Extend] = {
      key match {
        // rule 1: if original key was a modulo key, create a modulo key with the shape type extended but the
        // modulo type retained
        case mk: ModuloKey[_, _] => ModuloKey(compositeType, mk.moduloTypeInfo.cast[Any])

        // rule 2:  if original key was a super key, create a super key with the new shape type
        case sk: SuperKey[_] => SuperKey[From with Extend]

        // rule 3:  if original key was an auto key, create an auto key with the key type extended (note: that
        // will include all of the new fields, but not necessarily all of the original shape type fields, only those
        // that were in the original key
        case ak: AutoKey[_] => AutoKey[From with Extend]

        // rule 4: for all other cases, fallback to CompositeKey on SuperKey.
        case _ => CompositeKey[From, Extend, RelationKey[From], RelationKey[Extend]](key, SuperKey[Extend])
      }
    }

    def mkFlatten[Left: TypeInfo, Right: TypeInfo](key: RelationKey[(Left, Right)])(implicit
        flattenType: TypeInfo[Left with Right]): RelationKey[Left with Right] = {
      SuperKey[Left with Right]
    }

    def mkUnion(types: Iterable[TypeInfo[_]], keys: Iterable[RelationKey[_]]): (TypeInfo[_], RelationKey[_]) = {
      val (entityTypes, otherTypes) = types.partition(_ <:< classOf[Entity])
      if (otherTypes.isEmpty) {
        val superType = entityTypes.reduce(TypeInfo.getCommonLST)
        if (superType <:< classOf[Entity]) (superType, mkKey(superType))
        else throw new RelationalException("Cannot build common super entity type")
      } else if (entityTypes.nonEmpty) {
        throw new RelationalException(
          "Invalid union operation, Relation policy does not support union entity with plain class/trait")
      } else {
        val headType = types.head
        types.foreach(t =>
          if (!(t =:= headType))
            throw new RelationalException(s"Different TypeInfo: left [$headType] and right [$t]."))
        val headKey = keys.head
        keys.foreach(k =>
          if (!RelationalTreeUtils.compareKeys(headKey, k))
            throw new RelationalException(s"Different primary key: left [$headKey] and right [$k]."))
        (headType, headKey)
      }
    }

    def mkUpcast[From: TypeInfo, To >: From: TypeInfo](key: RelationKey[From]): RelationKey[To] = {
      val fromType = typeInfo[From]
      val toType = typeInfo[To]

      if (toType <:< classOf[Entity])
        mkKey[To]
      else if (toType =:= fromType)
        key.asInstanceOf[RelationKey[To]]
      else
        throw new RelationalException(s"The $toType must be an entity or equal to the previous type")
    }

    def mkDynamic[From: TypeInfo](key: RelationKey[From], field: String): RelationKey[DynamicObject] = {
      if (field == null) new DynamicFromStaticKey(key)
      else new DynamicExtendedKey(key, field)
    }

    def mkDynamic(fields: Seq[String], upperBound: TypeInfo[_]): RelationKey[Any] = {
      DynamicKey(fields, upperBound)
    }
  }
  case object Legacy extends Legacy

  trait NoKey extends KeyPropagationPolicy {
    def mkKey[T: TypeInfo]: RelationKey[T] = NK
    def mkProject[From: TypeInfo, To: TypeInfo](key: RelationKey[From]): RelationKey[To] = NK
    def mkTuple[From1: TypeInfo, From2: TypeInfo](
        key1: RelationKey[From1],
        key2: RelationKey[From2]): RelationKey[(From1, From2)] = NK
    def mkPartialTuple[From1: TypeInfo, From2: TypeInfo](
        key: Either[RelationKey[From1], RelationKey[From2]]): RelationKey[(From1, From2)] = NK
    def mkComposite[From: TypeInfo, Extend: TypeInfo](key: RelationKey[From])(implicit
        compositeType: TypeInfo[From with Extend]): RelationKey[From with Extend] = NK
    def mkFlatten[Left: TypeInfo, Right: TypeInfo](key: RelationKey[(Left, Right)])(implicit
        flattenType: TypeInfo[Left with Right]): RelationKey[Left with Right] = NK
    def mkUnion(types: Iterable[TypeInfo[_]], keys: Iterable[RelationKey[_]]): (TypeInfo[_], RelationKey[_]) = {
      (types.reduce(TypeInfo.getCommonLST), NK)
    }
    def mkUpcast[From: TypeInfo, To >: From: TypeInfo](key: RelationKey[From]): RelationKey[To] = NK
    def mkDynamic[From: TypeInfo](key: RelationKey[From], field: String): RelationKey[DynamicObject] = NK
    def mkDynamic(fields: Seq[String], upperBound: TypeInfo[_]): RelationKey[Any] = NK
  }
  case object NoKey extends NoKey

  trait EntityAndDimensionOnly extends KeyPropagationPolicy {
    def mkKey[T: TypeInfo]: RelationKey[T] = {
      val itemType = typeInfo[T]
      if (itemType <:< classOf[Entity]) {
        // if the shape is an entity, we can use an EntityKey
        val info = EntityInfoRegistry.getInfo(itemType.clazz)
        EntityKey[Storable](info)(itemType.cast[Storable]).asInstanceOf[RelationKey[T]]
      } else mkDimension[T]
    }

    def mkProject[From: TypeInfo, To: TypeInfo](key: RelationKey[From]): RelationKey[To] = {
      val fromType = typeInfo[From]
      val toType = typeInfo[To]
      if (fromType =:= toType) key.asInstanceOf[RelationKey[To]]
      else mkKey[To]
    }

    def mkComposite[From: TypeInfo, Extend: TypeInfo](key: RelationKey[From])(implicit
        compositeType: TypeInfo[From with Extend]): RelationKey[From with Extend] = {
      key match {
        case NK                  => NK
        case dk: DimensionKey[_] => mkDimension[From with Extend]
        case _ =>
          mkKey[Extend] match {
            case NK => key
            case ek => CompositeKey[From, Extend, RelationKey[From], RelationKey[Extend]](key, ek)
          }
      }
    }

    def mkFlatten[Left: TypeInfo, Right: TypeInfo](key: RelationKey[(Left, Right)])(implicit
        flattenType: TypeInfo[Left with Right]): RelationKey[Left with Right] = {
      key match {
        case NK => NK
        case tk: TupleKey[Left, Right, _, _] @unchecked =>
          (tk.k1, tk.k2) match {
            case (dk1: DimensionKey[_], dk2: DimensionKey[_]) => DimensionKey[Left with Right]
            case (k1, k2) => CompositeKey[Left, Right, RelationKey[Left], RelationKey[Right]](k1, k2)
          }
      }
    }

    def mkUnion(types: Iterable[TypeInfo[_]], keys: Iterable[RelationKey[_]]): (TypeInfo[_], RelationKey[_]) = {
      val superType = types.reduce(TypeInfo.getCommonLST)
      val allEntityKeys = keys.forall(k => k.isInstanceOf[EntityKey[_]])
      if (allEntityKeys) {
        if (superType <:< classOf[Entity])
          (superType, mkKey(superType))
        else
          throw new RelationalUnsupportedException(
            s"The deduced super type '$superType' should be @entity when all keys are entity keys")
      } else {
        val headKey = keys.head
        val allKeysEqual = keys.tail.forall(RelationalTreeUtils.compareKeys(headKey, _))
        val headType = types.head
        if (allKeysEqual) headKey match {
          case NK =>
            (superType, NK)
          case _ if types.tail.forall(t => t =:= headType) =>
            (headType, headKey)
          case dk: DimensionKey[_] if (superType.dimensionPropertyNames.toSet == headKey.fields.toSet) =>
            (superType, mkDimension(superType))
          case _ if (superType.propertyNames.intersect(headKey.fields).size == headKey.fields.size) =>
            (superType, mkDynamic(headKey.fields, superType))
          case _ =>
            throw new RelationalUnsupportedException(
              s"The deduced super type '$superType' does not contain all of the key fields: ${headKey.fields}")
        }
        else
          throw new RelationalUnsupportedException("Union candidates must use a same relation key")
      }
    }

    def mkUpcast[From: TypeInfo, To >: From: TypeInfo](key: RelationKey[From]): RelationKey[To] = {
      val fromType = typeInfo[From]
      val toType = typeInfo[To]

      if (fromType =:= toType) {
        key.asInstanceOf[RelationKey[To]]
      } else
        key match {
          case NK => NK
          case ek: EntityKey[_] =>
            if (toType <:< classOf[Entity])
              mkKey[To]
            else
              throw new RelationalUnsupportedException(
                s"The '$toType' should be @entity when previous key is entity key")
          case dk: DimensionKey[_] if (toType.dimensionPropertyNames.toSet == key.fields.toSet) => mkDimension[To]
          case _ if (toType.propertyNames.intersect(key.fields).size == key.fields.size) =>
            mkDynamic(key.fields, toType)
          case _ =>
            throw new RelationalUnsupportedException(
              s"The '$toType' does not contain all of the key fields: ${key.fields}")
        }
    }

    def mkTuple[From1: TypeInfo, From2: TypeInfo](
        key1: RelationKey[From1],
        key2: RelationKey[From2]): RelationKey[(From1, From2)] = {
      if (key1 == NK || key2 == NK) NK
      else TupleKey(key1, key2)
    }

    def mkPartialTuple[From1: TypeInfo, From2: TypeInfo](
        key: Either[RelationKey[From1], RelationKey[From2]]): RelationKey[(From1, From2)] = {
      key match {
        case Left(NK)  => NK
        case Left(key) => LeftKey[From1, From2, RelationKey[From1]](key)
        case _         => throw new RelationalException("Not implemented!")
      }
    }

    def mkDynamic[From: TypeInfo](key: RelationKey[From], field: String): RelationKey[DynamicObject] = {
      if (key == NK) NK
      else {
        if (field == null)
          new DynamicFromStaticKey(key)
        else
          new DynamicExtendedKey(key, field)
      }
    }

    def mkDynamic(fields: Seq[String], upperBound: TypeInfo[_]): RelationKey[Any] = {
      DynamicKey(fields, upperBound)
    }

    private def mkDimension[T: TypeInfo]: RelationKey[T] = {
      if (typeInfo[T].dimensionPropertyNames.isEmpty) NK else DimensionKey[T]
    }
  }
  case object EntityAndDimensionOnly extends EntityAndDimensionOnly
}

trait ProxyKeyPropagationPolicy extends KeyPropagationPolicy {
  val policy: KeyPropagationPolicy

  def mkKey[T: TypeInfo]: RelationKey[T] = {
    policy.mkKey[T]
  }

  def mkProject[From: TypeInfo, To: TypeInfo](key: RelationKey[From]): RelationKey[To] = {
    policy.mkProject[From, To](key)
  }

  def mkTuple[From1: TypeInfo, From2: TypeInfo](
      key1: RelationKey[From1],
      key2: RelationKey[From2]): RelationKey[(From1, From2)] = {
    policy.mkTuple[From1, From2](key1, key2)
  }

  def mkPartialTuple[From1: TypeInfo, From2: TypeInfo](
      key: Either[RelationKey[From1], RelationKey[From2]]): RelationKey[(From1, From2)] = {
    policy.mkPartialTuple[From1, From2](key)
  }

  def mkComposite[From: TypeInfo, Extend: TypeInfo](key: RelationKey[From])(implicit
      compositeType: TypeInfo[From with Extend]): RelationKey[From with Extend] = {
    policy.mkComposite[From, Extend](key)
  }

  def mkFlatten[Left: TypeInfo, Right: TypeInfo](key: RelationKey[(Left, Right)])(implicit
      flattenType: TypeInfo[Left with Right]): RelationKey[Left with Right] = {
    policy.mkFlatten[Left, Right](key)
  }

  def mkUnion(types: Iterable[TypeInfo[_]], keys: Iterable[RelationKey[_]]): (TypeInfo[_], RelationKey[_]) = {
    policy.mkUnion(types, keys)
  }

  def mkUpcast[From: TypeInfo, To >: From: TypeInfo](key: RelationKey[From]): RelationKey[To] = {
    policy.mkUpcast[From, To](key)
  }

  def mkDynamic[From: TypeInfo](key: RelationKey[From], field: String): RelationKey[DynamicObject] = {
    policy.mkDynamic[From](key, field)
  }

  def mkDynamic(fields: Seq[String], upperBound: TypeInfo[_]): RelationKey[Any] = {
    policy.mkDynamic(fields, upperBound)
  }
}
