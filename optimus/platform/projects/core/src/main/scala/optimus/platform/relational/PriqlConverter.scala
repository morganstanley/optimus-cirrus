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

import optimus.graph.AlreadyCompletedNode
import optimus.graph.NodeFuture
import optimus.platform.annotations.nodeSync
import optimus.platform.storable.EntityReference
import optimus.core.needsPlugin
import optimus.graph.Node
import optimus.platform.cm.Knowable
import optimus.platform.cm.Known

trait PriqlConverter[T, U] {
  @nodeSync def convert(t: T): U
  def convert$queued(t: T): NodeFuture[U] = needsPlugin
  def convert$newNode(t: T): Node[U] = needsPlugin
}

object PriqlConverter {
  implicit def optionConverter[T, U](implicit conv: PriqlConverter[T, U]): PriqlConverter[T, Option[U]] =
    new PriqlConverter[T, Option[U]] {
      @nodeSync def convert(t: T): Option[U] = convert$queued(t).get$
      override def convert$queued(t: T): NodeFuture[Option[U]] = {
        conv.convert$queued(t).asNode$.map(x => Option(x)).enqueue
      }
    }

  implicit def knowableConverter[T, U](implicit conv: PriqlConverter[T, U]): PriqlConverter[T, Knowable[U]] =
    new PriqlConverter[T, Knowable[U]] {
      @nodeSync def convert(t: T): Knowable[U] = convert$queued(t).get$
      override def convert$queued(t: T): NodeFuture[Knowable[U]] = {
        conv.convert$queued(t).asNode$.map(x => Known(x)).enqueue
      }
    }

  implicit def identityConverter[T, U >: T]: PriqlConverter[T, U] = new PriqlConverter[T, U] {
    @nodeSync def convert(t: T): U = t
    override def convert$queued(t: T): NodeFuture[U] = new AlreadyCompletedNode(t)
  }
}

trait PriqlReferenceConverter[T] {
  @nodeSync def toReference(t: T): EntityReference
  def toReference$queued(t: T): NodeFuture[EntityReference] = needsPlugin
}
