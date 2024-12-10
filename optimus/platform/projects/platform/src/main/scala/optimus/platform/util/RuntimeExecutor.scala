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
package optimus.platform.util

import optimus.core.utils.RuntimeMirror

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

object RuntimeExecutor {
  val tb = RuntimeMirror.forClass(getClass).mkToolBox()

  /**
   * WARNING: When you use this method, we can't verify any of code at compile time, so you must test the places using
   * this method and make sure these doesn't have any security problem! If users get to enter the code then they can't
   * be given functions that might cause damage! E.g. if the 'Data' class has a method 'deleteAllOurData' then it
   * probably shouldn't be exposed to an end user.
   *
   * If the input is null or empty string, then return null Else, compile the code and run it with the passed instance
   *
   * This method will throw exception when run the code
   */
  def execute[T](code: String, inst: T)(implicit tag: WeakTypeTag[T]): Any = {
    if (code == null || code.isEmpty) return null

    //    val runCode = replaceCode(code)
    val instTree = reify(inst).tree

    try {
      val evalTree = replaceTree(tb.parse(code), instTree)
      //    println(show(evalTree))

      tb.eval(evalTree)
    } catch {
      case ex: Exception  => ??? // throw msjava.base.lang.ExceptionUtils.getUltimateCause(ex)
      case err: Throwable => throw new Exception("Dynamic code compile error! Please check your expression: " + code)
    }

  }

  // put the instance into the AST
  private def replaceTree(tree: Tree, instTree: Tree): Tree = tree match {
    case Apply(fun, args)            => Apply(replaceTree(fun, instTree), args map (arg => replaceTree(arg, instTree)))
    case TypeApply(inst, tpes)       => TypeApply(replaceTree(inst, instTree), tpes)
    case Select(qualifier, selector) => Select(replaceTree(qualifier, instTree), selector)
    case Ident(term)                 => Select(instTree, term) // here do the replace job
    case Literal(l)                  => Literal(l)

    case _ => throw new Exception("Not support operation")
  }
}
