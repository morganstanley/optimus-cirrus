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
package optimus.stratosphere.common

import scala.language.experimental.macros
import scala.reflect.macros.whitebox

/**
 * Namer provide `named` macro that turn `val valname = named` into `val valname = "valname"`. Basically to use in
 * 'Names' kind of objects (where we have string vals that should have same name as value
 */
trait Namer {
  def named: String = macro Namer.namedImplementation
}
object Namer {
  private val help = "Supported tree shape: final val <name> = named`."
  def namedImplementation(c: whitebox.Context): c.Tree = {
    c match {
      case mContext: scala.reflect.macros.contexts.Context =>
        mContext.callsiteTyper.context.enclosingContextChain.headOption.map(_.tree) match {
          case Some(mContext.universe.ValDef(_, name, _, _)) =>
            c.typecheck(c.universe.Literal(c.universe.Constant(name.decodedName.toString)))
          case other =>
            c.abort(c.enclosingPosition, s"Unrecognized tree: $other. $help")
        }

      case _ =>
        c.abort(c.enclosingPosition, s"Unable to find enclosing tree. $help")
    }
  }
}
