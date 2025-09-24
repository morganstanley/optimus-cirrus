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

import optimus.platform.embeddable
import optimus.platform.storable.Embeddable
import optimus.platform.versioning.FieldType

import scala.annotation.unused
import scala.util.Try
import scala.util.Success
import scala.reflect.runtime.universe._

/**
 * Provides a mechanism for custom pickling and unpickling of embeddable types. It should only be used when absolutely
 * necessary, primarily for supporting backward compatibility for data already saved in the DAL.
 *
 * In order to use customPickling, an @embeddable must declare an object type that extends `CustomPicklingSpec[T]`.
 * `CustomPicklingSpec[T]` is used to:
 *  i) Supply custom implementations of `Pickler[T]` and `Unpickler[T]`
 *  ii) Inform the optimus versioning infrastructure with what pickled type the custom pickling logic resolves to. This
 *      allows the versioning infra to correctly match shaps to invoke the correct versioning transformers. Typically
 *      the default `FieldType.IterableOrString` is used but implementations are allowed to use other forms.
 *
 *  In most cases the provided object would extend `CustomPickling[T]` which implements `CustomPicklingSpec[T]` by
 *  extending Pickler[T] and Unpickler[T] such that derived classes only need to implement the pickle and unpickle
 *  methods defined in these traits.
 *
 *  Where the custom pickling implementation requires knowledge of any inner types (for example, to resolve their
 *  picklers via the `Registry`), the supplied object needs to directly extend `CustomPicklingSpec[T]` and implement
 *  the `pickler` and `unpickler` methods in order to obtain the inner types.
 *
 * 5- Reference this object in the embeddable annotation:
 * {{{
 *   @embeddable(customPickling = classOf[YourCustomPickling.type])
 * }}}
 *
 * The optimus runtime will automatically detect and use your custom implementation when pickling or
 * unpickling instances of the type.
 *
 * @example
 * {{{
 * @embeddable(customPickling = classOf[FooWithCustomPickling.type])
 * final case class FooWithCustomPickling(i: Int)
 *
 * object FooWithCustomPickling extends CustomPickling[FooWithCustomPickling] {
 *   override def pickle(t: FooWithCustomPickling, visitor: PickledOutputStream): Unit = {
 *     visitor.writeRawObject((t.i * 2).asInstanceOf[AnyRef])
 *   }
 *   override def unpickle(pickled: Any, stream: PickledInputStream): FooWithCustomPickling = {
 *     FooWithCustomPickling(pickled.asInstanceOf[Int] / 2)
 *   }
 * }
 * }}}
 *
 * @note The default implementation of pickle falls back to using the default pickler for the class.
 *
 * @tparam T The type for which custom pickling is being defined
 */
trait CustomPicklingSpec[T] {

  /**
   * Override this method if the pickled form is of a FieldType other
   * than FieldType.IterableOrString.
   */
  def pickledType: FieldType = FieldType.IterableOrString
  def pickler(tpe: Type): Pickler[T]
  def unpickler(tpe: Type): Unpickler[T]
}

trait CustomPickling[T] extends CustomPicklingSpec[T] with Pickler[T] with Unpickler[T] {
  def pickler(@unused tpe: Type): Pickler[T] = this
  def unpickler(@unused tpe: Type): Unpickler[T] = this
}

class CustomPicklingImplementationException(msg: String, cause: Option[Exception] = None)
    extends PicklingException(msg, cause)

object CustomPicklingSpec {

  def picklerOfType(tpe: Type): Option[Pickler[_]] = ofType(tpe).map { _.pickler(tpe) }

  private def ofType(tpe: Type): Option[CustomPicklingSpec[_]] = {
    if (tpe <:< typeOf[Embeddable]) {
      tpe match {
        case RefinedType(types: List[Type], _) =>
          val cps: Seq[CustomPicklingSpec[_]] = types.flatMap { ofType }
          if (cps.isEmpty) None
          else if (cps.size == 1) Some(cps.head)
          else
            throw new CustomPicklingImplementationException(
              s"Refined type $tpe results in ${cps.size}" +
                "CustomPicklingSpec[T] instances. Consider extending the defined type and declare a custom pickling" +
                "for that")
        case _ =>
          val m = runtimeMirror(getClass.getClassLoader)
          CustomPicklingSpec.ofClass(m.runtimeClass(tpe))
      }
    } else None
  }

  def unpicklerOfType(tpe: Type): Option[Unpickler[_]] = ofType(tpe).map { _.unpickler(tpe) }

  def ofClass(cls: Class[_]): Option[CustomPicklingSpec[_]] = {
    val emb = cls.getAnnotation(classOf[embeddable])
    if (emb != null) {
      val cp = emb.customPickling()
      def badImpl(): Nothing = throw new CustomPicklingImplementationException(
        s"Specified customPickling class ${cp.getName} on @embeddable ${cls.getName} must be an object and extend " +
          s"CustomPicklingSpec[_]")
      if (classOf[CustomPicklingSpec[_]].isAssignableFrom(cp)) {
        Try {
          cp.getField("MODULE$")
        } match {
          case Success(m) => Some(m.get(null).asInstanceOf[CustomPicklingSpec[_]])
          case _          => badImpl()
        }
      } else if (cp != classOf[Void]) badImpl()
      else None
    } else None
  }
}
