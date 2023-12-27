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
package optimus.buildtool.resolvers

import java.util.concurrent.ExecutorService
import coursier.util.Sync
import optimus.core.CoreAPI
import optimus.graph.AlreadyCompletedNode
import optimus.graph.AlreadyCompletedOrFailedNode
import optimus.graph.Node
import optimus.graph.Scheduler
import optimus.platform.NodeTryNode
import optimus.platform._

import scala.annotation.nowarn

object CoursierGraphAdaptor extends Sync[Node] {
  override def point[A](a: A): Node[A] = new AlreadyCompletedNode(a)
  override def bind[A, B](elem: Node[A])(f: A => Node[B]): Node[B] = elem.flatMap(f)
  override def map[A, B](elem: Node[A])(f: A => B): Node[B] = elem.map(f)
  override def gather[A](elems: scala.Seq[Node[A]]): Node[scala.Seq[A]] =
    CoreAPI.nodify(elems.apar.map(asyncGet)).enqueue
  override def delay[A](a: => A): Node[A] = CoreAPI.nodify(a)
  override def fromAttempt[A](a: Either[Throwable, A]): Node[A] = AlreadyCompletedOrFailedNode(a)

  override def handle[A](a: Node[A])(f: PartialFunction[Throwable, A]): Node[A] =
    throw new UnsupportedOperationException("Use attempt rather than directly calling handle")

  /**
   * This is a "benign" sync stack because f is already executed by the time attempt() is called, hence the .toTry
   * doesn't need to wait on anything. [[NodeTry.toTry]] is async for propagation reasons only.
   */
  @nowarn("msg=17001") override def attempt[A](f: Node[A]): Node[Either[Throwable, A]] =
    new NodeTryNode[A](f).map(_.toTry.toEither)

  override def schedule[A](pool: ExecutorService)(f: => A): Node[A] = {
    // we ignore the pool and always schedule on the graph scheduler
    val node = CoreAPI.nodify(f)
    Scheduler.currentOrDefault.evaluateNodeAsync(node)
    node
  }
}
