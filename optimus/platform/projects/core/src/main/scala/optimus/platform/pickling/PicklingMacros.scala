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

import optimus.graph.PicklingMacroUtils

object PicklingMacros {
  import scala.reflect.macros.blackbox.Context

  def findPickler[T](t: T): Pickler[T] = macro findPicklerImpl[T]

  def findPicklerImpl[T: c.WeakTypeTag](c: Context)(t: c.Expr[T]): c.Expr[Pickler[T]] = {
    val PicklingMacroUtils = new PicklingMacroUtils[c.type](c)
    c.Expr[Pickler[T]] {
      PicklingMacroUtils.selectRegistryPicklerMethod(c.weakTypeOf[T])
    }
  }

}
