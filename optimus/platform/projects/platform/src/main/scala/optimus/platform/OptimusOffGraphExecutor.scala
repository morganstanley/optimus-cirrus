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
package optimus.platform

import optimus.breadcrumbs.crumbs.RequestsStallInfo
import optimus.breadcrumbs.crumbs.StallPlugin
import optimus.core.StallInfoAppender
import optimus.graph.GraphStallInfo
import optimus.graph.Node
import optimus.graph.NodePromise
import optimus.graph.PluginType
import optimus.platform.annotations._

import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Failure
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Allows insertion of non-optimus code into the optimus graph without stalling an optimus worker thread. The main use
 * case for this would be IO performed from within a node. As always this sort of pattern should be avoided and we
 * should instead try to use DAL as our communication mechanism. But in the limited use cases where it is necessary this
 * is ok.
 *
 * The Default executor uses a thread pool which will create up to 50 threads to process the work coming in. It is
 * assumed that the work we push off graph is not CPU intensive and that there are not excessive numbers of outstanding
 * calls created. For a use case that would generate a lot of traffic, it may be necessary to use a custom
 * ExecutorService or otherwise throttle though tools like AdvancedUtils.Throttle
 */
object OptimusOffGraphExecutor {

  private val coreThreads = sys.props.getOrElse("optimus.platform.OptimusOffGraphExecutor.coreThreads", "50").toInt
  private val coreThreadTimeoutMs =
    sys.props.getOrElse("optimus.platform.OptimusOffGraphExecutor.coreThreadTimeoutMs", "1000").toInt

  def create(service: ExecutorService, name: String) = OptimusOffGraphExecutor(service, PluginType(name))

  lazy val Default = {
    val counter = new AtomicInteger(1)
    val ex = new ThreadPoolExecutor(
      coreThreads,
      coreThreads,
      coreThreadTimeoutMs,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable],
      (r: Runnable) =>
        new Thread(r) {
          setName(s"OptimusOffGraphExecutor-${counter.getAndIncrement()}")
          setDaemon(true)
        }
    )
    ex.allowCoreThreadTimeOut(true)
    create(ex, "DefaultOptimusOffGraphExecutor")
  }

  /**
   * execute f asynchronously on a non-graph thread. on graph stalls, extra context can display request ids or other
   * additional information
   */
  @async def executeAsync[T](f: () => T, context: Option[String] = None): T = Default.executeAsync(f, context)

  /**
   * trigger work asynchronously for off-graph completion. Generally this method is aimed towards cases where an IO call
   * is using its own asynchronous execution and we want to forward that capability to Optimus
   *
   * f is executed eagerly on a non-graph thread. The job of f is to submit whatever work is necessary to complete the
   * task. The single argument to f is a callback function that allows the node to be completed.
   * executeAsyncCallback[String]{ callback => callIOInfra( callback ) )
   */
  @async def executeAsyncCallback[T](f: NodePromise[T] => Unit, context: Option[String] = None): T =
    Default.executeAsyncCallback(f, context)

}

@entity
class OptimusOffGraphExecutor(private[platform] val service: ExecutorService, val pluginType: PluginType) {

  @async
  def executeAsync[T](f: () => T, context: Option[String] = None): T =
    executeAsyncCallback[T](_.complete(Try { f() }), context)

  @nodeSync
  private def executeAsyncCallback[T](f: NodePromise[T] => Unit, context: Option[String] = None): T =
    executeAsyncCallback$withNode(f, context)
  private def executeAsyncCallback$queued[T](f: NodePromise[T] => Unit, context: Option[String] = None): Node[T] = {
    val promise = NodePromise[T](pluginType.reportingInfo)
    service.submit({ () =>
      {
        try {
          f(promise)
        } catch {
          case NonFatal(t) => promise.complete(Failure(t))
        }
      }
    }: Runnable)
    StallInfoAppender.attachExtraData(
      promise.underlyingNode,
      () =>
        GraphStallInfo(
          pluginType,
          "OffGraphExecutor asynchronously executing",
          context.map { ctx => RequestsStallInfo(StallPlugin.DAL, 1, Seq(ctx)) })
    )
    promise.underlyingNode
  }
  private def executeAsyncCallback$withNode[T](f: NodePromise[T] => Unit, context: Option[String] = None): T =
    executeAsyncCallback$queued(f, context).get
}
