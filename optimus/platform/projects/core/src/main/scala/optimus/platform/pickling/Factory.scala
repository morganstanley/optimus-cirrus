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
package optimus.platform.pickling

import scala.reflect.runtime.universe._
import PicklingReflectionUtils._

/**
 * Interface for a Factory that can create values of type Pickler
 */
trait PicklerFactory extends Factory[Pickler] {

  protected case class PicklerWithManifest(tpe: Type) {
    lazy val pickler: Pickler[Any] = Registry.picklerOfType(tpe).asInstanceOf[Pickler[Any]]
    lazy val manifest: Manifest[Any] = manifestOf(tpe)
  }

  type ValueWrapper = PicklerWithManifest

  protected val wrapValue: Type => PicklerWithManifest = PicklerWithManifest.apply
}

/**
 * Interface for a Factory that can create values of type Unpickler
 */
trait UnpicklerFactory extends Factory[Unpickler] {

  protected case class UnpicklerWithManifest(tpe: Type) {
    lazy val unpickler: Unpickler[Any] = Registry.unpicklerOfType(tpe).asInstanceOf[Unpickler[Any]]
    lazy val manifest: Manifest[Any] = manifestOf(tpe)
  }

  type ValueWrapper = UnpicklerWithManifest

  protected val wrapValue: Type => UnpicklerWithManifest = UnpicklerWithManifest.apply

  protected def orderingOf(tpe: Type): Ordering[Any] = Registry.orderingOfType(tpe).asInstanceOf[Ordering[Any]]
}

/**
 * Interface for a Factory that can create values of type Ordering
 */
trait OrderingFactory extends Factory[Ordering] {
  type ValueWrapper = Ordering[_]

  protected val wrapValue: Type => Ordering[_] = Registry.orderingOfType
}

/**
 * It identifies factories that contain basic types rules, such as rules to generate picklers and unpicklers for
 * entities and embeddables
 */
protected[pickling] trait DefaultFactoryTrait
trait DefaultFactory[Value[_]] extends DefaultFactoryTrait { self: Factory[Value] => }

/**
 * A Factory tries to create a value for a given type by applying predefined rules.
 *
 * A factory has three type of pre-defined rules: 1) Rules that match a class 2) Rules that match any superclasses of a
 * class 3) Rules that match any superclass of a companion
 *
 * @tparam Value,
 *   the type of values it needs to create
 */
trait Factory[Value[_]] {

  type ValueWrapper
  type ClassKey = String

  protected def wrapValue: Type => ValueWrapper

  /**
   * It matches any class that has ClassKey as class.
   *
   * List[Type] represents the list of type arguments of a type. Examples are following:
   *   - `String` has no type arguments,
   *   - `Option[String]` has one type argument: `String`
   *   - `Map[Int, Long]` has two type arguments: `Int` and `Long` We return an Option[Value[_]] as we may want to
   *     generate a value only for a class for specific type args
   */
  protected def values: Map[ClassKey, List[Type] => Option[Value[_]]]

  /**
   * It matches any class that has ClassKey as superclass.
   *
   * It generates a lambda to generate Value[_] based of its type.
   */
  protected def superclassValues: Map[ClassKey, Type => Option[Value[_]]] = Map.empty
  private lazy val allowedSuperclasses = superclassValues.keySet

  final def createInstanceFromClass(tpe: Type, key: String): Option[Value[_]] = {
    values.get(key).flatMap(f => f(getTypeArgs(tpe)))
  }

  final def createInstanceFromSuperclass(tpe: Type, superclassKeys: List[String]): Option[Value[_]] = {
    val matchedKey = superclassKeys.find(allowedSuperclasses.contains)
    matchedKey.flatMap(key => superclassValues(key)(tpe))
  }

  protected def classKeyOf[T: TypeTag]: ClassKey = {
    val tpe = typeOf[T]
    if (tpe =:= typeOf[Enumeration#Value]) "scala.Enumeration.Value" // enums have a custom key in factories
    else extractName(tpe)
  }

  protected def typesMatch(tpeA: Type, tpeB: Type): Boolean = tpeA =:= tpeB

  protected def typeParams0(value: => Value[_])(typeArgs: List[Type]): Option[Value[_]] = typeArgs match {
    case Nil   => Some(value)
    case types => explodeWithWrongTypeArgs(types, expected = 0)
  }

  protected def typeParams1(func: ValueWrapper => Value[_])(typeArgs: List[Type]): Option[Value[_]] = typeArgs match {
    case List(t) => Some(func(wrapValue(t)))
    case types   => explodeWithWrongTypeArgs(types, expected = 1)
  }

  protected def typeParams2(func: (ValueWrapper, ValueWrapper) => Value[_])(typeArgs: List[Type]): Option[Value[_]] =
    typeArgs match {
      case List(a, b) => Some(func(wrapValue(a), wrapValue(b)))
      case types      => explodeWithWrongTypeArgs(types, expected = 2)
    }

  protected def typeParams3(func: (ValueWrapper, ValueWrapper, ValueWrapper) => Value[_])(
      typeArgs: List[Type]): Option[Value[_]] =
    typeArgs match {
      case List(a, b, c) => Some(func(wrapValue(a), wrapValue(b), wrapValue(c)))
      case types         => explodeWithWrongTypeArgs(types, expected = 3)
    }

  protected def typeParams4(func: (ValueWrapper, ValueWrapper, ValueWrapper, ValueWrapper) => Value[_])(
      typeArgs: List[Type]): Option[Value[_]] =
    typeArgs match {
      case List(a, b, c, d) => Some(func(wrapValue(a), wrapValue(b), wrapValue(c), wrapValue(d)))
      case types            => explodeWithWrongTypeArgs(types, expected = 4)
    }

  protected def typeParams5(func: (ValueWrapper, ValueWrapper, ValueWrapper, ValueWrapper, ValueWrapper) => Value[_])(
      typeArgs: List[Type]): Option[Value[_]] =
    typeArgs match {
      case List(a, b, c, d, e) =>
        Some(func(wrapValue(a), wrapValue(b), wrapValue(c), wrapValue(d), wrapValue(e)))
      case types => explodeWithWrongTypeArgs(types, expected = 5)
    }

  protected def typeParams6(
      func: (ValueWrapper, ValueWrapper, ValueWrapper, ValueWrapper, ValueWrapper, ValueWrapper) => Value[_])(
      typeArgs: List[Type]): Option[Value[_]] =
    typeArgs match {
      case List(a, b, c, d, e, f) =>
        Some(func(wrapValue(a), wrapValue(b), wrapValue(c), wrapValue(d), wrapValue(e), wrapValue(f)))
      case types => explodeWithWrongTypeArgs(types, expected = 6)
    }

  protected def typeParams7(
      func: (ValueWrapper, ValueWrapper, ValueWrapper, ValueWrapper, ValueWrapper, ValueWrapper, ValueWrapper) => Value[
        _])(typeArgs: List[Type]): Option[Value[_]] = typeArgs match {
    case List(a, b, c, d, e, f, g) =>
      Some(func(wrapValue(a), wrapValue(b), wrapValue(c), wrapValue(d), wrapValue(e), wrapValue(f), wrapValue(g)))
    case types => explodeWithWrongTypeArgs(types, expected = 7)
  }

  protected def typeParams8(
      func: (
          ValueWrapper,
          ValueWrapper,
          ValueWrapper,
          ValueWrapper,
          ValueWrapper,
          ValueWrapper,
          ValueWrapper,
          ValueWrapper) => Value[_])(typeArgs: List[Type]): Option[Value[_]] = typeArgs match {
    case List(a, b, c, d, e, f, g, h) =>
      Some(
        func(
          wrapValue(a),
          wrapValue(b),
          wrapValue(c),
          wrapValue(d),
          wrapValue(e),
          wrapValue(f),
          wrapValue(g),
          wrapValue(h)))
    case types => explodeWithWrongTypeArgs(types, expected = 8)
  }
  protected def typeParams9(
      func: (
          ValueWrapper,
          ValueWrapper,
          ValueWrapper,
          ValueWrapper,
          ValueWrapper,
          ValueWrapper,
          ValueWrapper,
          ValueWrapper,
          ValueWrapper) => Value[_])(typeArgs: List[Type]): Option[Value[_]] = typeArgs match {
    case List(a, b, c, d, e, f, g, h, i) =>
      Some(
        func(
          wrapValue(a),
          wrapValue(b),
          wrapValue(c),
          wrapValue(d),
          wrapValue(e),
          wrapValue(f),
          wrapValue(g),
          wrapValue(h),
          wrapValue(i)
        ))
    case types => explodeWithWrongTypeArgs(types, expected = 9)
  }

  protected def typeParams10(
      func: (
          ValueWrapper,
          ValueWrapper,
          ValueWrapper,
          ValueWrapper,
          ValueWrapper,
          ValueWrapper,
          ValueWrapper,
          ValueWrapper,
          ValueWrapper,
          ValueWrapper) => Value[_])(typeArgs: List[Type]): Option[Value[_]] = typeArgs match {
    case List(a, b, c, d, e, f, g, h, i, l) =>
      Some(
        func(
          wrapValue(a),
          wrapValue(b),
          wrapValue(c),
          wrapValue(d),
          wrapValue(e),
          wrapValue(f),
          wrapValue(g),
          wrapValue(h),
          wrapValue(i),
          wrapValue(l)
        ))
    case types => explodeWithWrongTypeArgs(types, expected = 10)
  }

  private def explodeWithWrongTypeArgs(actual: List[Type], expected: Int) =
    throw new IllegalStateException(
      s"Expected $expected type arguments, found ${actual.size}. Type arguments were ${actual.mkString("[", ",", "]")}")

  def classKeys: Set[ClassKey] = values.keySet
}
