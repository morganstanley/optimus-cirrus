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
package optimus.platform.internal

import scala.reflect.macros.blackbox.Context

trait MacroBase extends WithOptimusMacroReporter {
  val c: Context

  import c.universe._

  def mkList[T](stats: List[c.Expr[T]]): c.Expr[List[T]] = {
    c.Expr[List[T]](q"_root_.scala.List.apply(..$stats)")
  }

  private def accessedOrSelf(s: Symbol) = if (s.isTerm && s.asTerm.isAccessor) s.asTerm.accessed orElse s else s

  private def hasAnnotation[T: c.WeakTypeTag](sym: Symbol): Boolean = {
    val weakType = c.weakTypeOf[T].dealias
    sym.annotations exists (_.tree.tpe =:= weakType)
  }

  private def findAnnotation[T: c.WeakTypeTag](sym: Symbol): Option[Annotation] = {
    val weakType = c.weakTypeOf[T].dealias
    sym.annotations find (_.tree.tpe =:= weakType)
  }

  // check tree contains annotation
  def hasAnnotation[T: c.WeakTypeTag](tree: Tree): Boolean = {
    hasAnnotation(tree.symbol) || {
      val acc = accessedOrSelf(tree.symbol)
      (acc != tree.symbol) && hasAnnotation(acc)
    }
  }
  // check tree contains annotation
  def getAnnotation[T: c.WeakTypeTag](tree: Tree): Option[Annotation] = {

    findAnnotation(tree.symbol).orElse {
      val acc = accessedOrSelf(tree.symbol)
      if (acc == tree.symbol) None else findAnnotation(acc)
    }
  }

  // implicit converter finder
  @inline def findConverter[From: c.WeakTypeTag, To: c.WeakTypeTag] = c.inferImplicitValue(c.weakTypeOf[From => To])

}
