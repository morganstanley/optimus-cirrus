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
package optimus.tools.scalacplugins.entity

import scala.tools.nsc._
import scala.tools.nsc.symtab.Flags

trait TreeDuplicator {
  val global: Global

  import global._

  // there is a private[scala] duplicator in Trees which is in the inheritance closure of Global.
  // XXX: file bug report with TypeSafe to either remove duplicator (it's not used in stock scalac)
  // or expose it to subclasses for use
  object treeDuplicator extends Transformer {
    override val treeCopy = new StrictTreeCopier
    override def transform(t: Tree): Tree = {
      val t1 = super.transform(t)
      if ((t1 ne t) && t1.pos.isRange) t1 setPos t.pos.focus
      t1
    }
    def transformTyped[T <: Tree](t: T): T = transform(t).asInstanceOf[T]

    def transformListOfLists(lst: List[List[ValDef]]): List[List[ValDef]] = lst map (_ map transformTyped)
    def transformList[T <: Tree](lst: List[T]): List[T] = lst map transformTyped
  }

  def dupTree[T <: Tree](t: T): T = treeDuplicator transformTyped t
  def dupTree[T <: Tree](lst: List[T]): List[T] = treeDuplicator transformList lst
  def dupTreeListList[T <: Tree](lst: List[List[T]]): List[List[T]] = lst map (l => treeDuplicator transformList l)
  def dupTreeFlat(lst: List[List[ValDef]]): List[ValDef] = (treeDuplicator transformListOfLists lst).flatten

  def dupTypeDefsAsInvariant(tparams: List[TypeDef]): List[TypeDef] = {
    treeDuplicator transformTypeDefs tparams map { td =>
      treeCopy.TypeDef(td, td.mods &~ (Flags.COVARIANT | Flags.CONTRAVARIANT), td.name, td.tparams, td.rhs)
    }
  }
}
