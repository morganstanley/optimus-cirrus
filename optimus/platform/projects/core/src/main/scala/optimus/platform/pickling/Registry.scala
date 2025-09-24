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

object Registry {

  private val duringCompilation = Option(System.getProperty("optimus.inCompiler")).exists(_.toBoolean)

  // A bit of a hack here! The versioning macro uses embeddable entities,
  // so we need to avoid resolving picklers when we are compiling.
  private def runtimeOnly[T](t: => T): T = if (duringCompilation) null.asInstanceOf[T] else t

  private val picklerRegistry = runtimeOnly(new PicklerRegistry)
  private val unpicklerRegistry = runtimeOnly(new UnpicklerRegistry)
  private val orderingRegistry = runtimeOnly(new OrderingRegistry)

  final def picklerOf[T: TypeTag]: Pickler[T] = {
    // Yes, we're casting but better than repeating the code.
    picklerOfType(typeOf[T]).asInstanceOf[Pickler[T]]
  }

  final def picklerOfType(tpe: Type): Pickler[_] = {
    runtimeOnly(picklerRegistry.lazyInstanceFor(tpe, CustomPicklingSpec.picklerOfType).eval)
  }

  // For safely building types with one or more type parameters
  final def picklerOfAppliedType[T](weakTypeTag: WeakTypeTag[T], arg0: Type, args: Type*): Pickler[_] = runtimeOnly {
    val tpe = PicklingReflectionUtils.safeAppliedType(weakTypeTag, (arg0 +: args).toList)
    picklerOfType(tpe)
  }

  // NOTE: Use only for classes with no type parameters!
  final def unsafePicklerOfClass[T](clazz: Class[T]): Pickler[T] = {
    requireClassWithNoTypeParams(clazz, methodName = "unsafePicklerOfClass", alternativeMethod = "picklerOf")
    runtimeOnly(picklerRegistry.unsafeLazyInstanceFor(clazz).eval)
  }

  final def unpicklerOf[T: TypeTag]: Unpickler[T] = {
    // Yes, we're casting but better than repeating the code.
    unpicklerOfType(typeOf[T]).asInstanceOf[Unpickler[T]]
  }

  final def unpicklerOfType(tpe: Type): Unpickler[_] = {
    runtimeOnly(unpicklerRegistry.lazyInstanceFor(tpe, CustomPicklingSpec.unpicklerOfType).eval)
  }

  // For safely building types with one or more type parameters
  final def unpicklerOfAppliedType[T](weakTypeTag: WeakTypeTag[T], arg0: Type, args: Type*): Unpickler[_] =
    runtimeOnly {
      val tpe = PicklingReflectionUtils.safeAppliedType(weakTypeTag, (arg0 +: args).toList)
      unpicklerOfType(tpe)
    }

  // NOTE: Use only for classes with no type parameters!
  final def unsafeUnpicklerOfClass[T](clazz: Class[T]): Unpickler[T] = {
    requireClassWithNoTypeParams(clazz, methodName = "unsafeUnpicklerOfClass", alternativeMethod = "unpicklerOf")
    runtimeOnly(unpicklerRegistry.unsafeLazyInstanceFor(clazz).eval)
  }

  final def orderingOfType(tpe: Type): Ordering[_] = runtimeOnly(orderingRegistry.lazyInstanceFor(tpe).eval)

  private def requireClassWithNoTypeParams[T](clazz: Class[T], methodName: String, alternativeMethod: String): Unit = {
    def msg = s"The method $methodName cannot be used for classes with type parameters. Use $alternativeMethod instead."
    require(clazz.getTypeParameters.isEmpty, msg)
  }
}
