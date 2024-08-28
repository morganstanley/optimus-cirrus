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
package optimus.graph

import optimus.platform.pickling.Registry

import scala.reflect.macros.blackbox.Context

/**
 * The Registry pattern relies on TypeTag to get the full type information of the object to (un)pickle. However typeTags
 * are extremely heavy, so for types that have no type parameters we rely on Class instead.
 */
class PicklingMacroUtils[C <: Context](val c: C) {
  import c.universe.internal.gen
  import c.universe._

  def selectRegistryPicklerMethod(tpe: Type): c.Tree =
    if (hasNoTypeParams(tpe) && !isScalaEnum(tpe))
      gen.mkMethodCall(registryMethodNamed("unsafePicklerOfClass"), mkClassOf(tpe) :: Nil)
    else gen.mkNullaryCall(registryMethodNamed("picklerOf"), tpe :: Nil)

  def selectRegistryUnpicklerMethod(tpe: Type): c.Tree =
    if (hasNoTypeParams(tpe) && !isScalaEnum(tpe))
      gen.mkMethodCall(registryMethodNamed("unsafeUnpicklerOfClass"), mkClassOf(tpe) :: Nil)
    else gen.mkNullaryCall(registryMethodNamed("unpicklerOf"), tpe :: Nil)

  private def isScalaEnum(tpe: Type): Boolean = tpe.dealias <:< typeOf[Enumeration#Value]
  private def hasNoTypeParams(tpe: Type): Boolean = tpe.dealias.typeArgs.isEmpty

  private val RegistryTpe = typeOf[Registry.type]
  private def registryMethodNamed(name: String) = RegistryTpe.decl(TermName(name))
  private def mkClassOf(tpe: Type) = Literal(Constant(tpe))
}
