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

import optimus.platform._
import optimus.platform.annotations.assumeParallelizableInClosure
import optimus.platform.relational.tree.ConstValueElement
import optimus.platform.relational.tree.ElementFactory
import optimus.platform.relational.tree.TypeInfo

final case class AsyncValueHolder[T](f: NodeFunction0[T]) {
  @async def evaluate: T = f.apply()

  @entersGraph def evaluateSync = f.apply()

  override def toString() = s"<async value>@${f.hashCode}"
}

object AsyncValueHolder {
  // used in LambdaReifier
  def constant[R](@assumeParallelizableInClosure f: () => R, typeInfo: TypeInfo[_]): ConstValueElement = {
    ElementFactory.constant(f(), typeInfo)
  }

  def constant$NF[R](@assumeParallelizableInClosure f: NodeFunction0[R], typeInfo: TypeInfo[_]): ConstValueElement = {
    ElementFactory.constant(AsyncValueHolder(f), typeInfo)
  }

  def map(v: Any, f: Any => Any): Any = {
    v match {
      case AsyncValueHolder(nf) =>
        AsyncValueHolder(asNode { () =>
          f(nf())
        })
      case _ => f(v)
    }
  }
}
