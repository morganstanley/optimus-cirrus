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
package optimus.examples.platform

import com.google.common.util.concurrent.ThreadFactoryBuilder
import optimus.graph.CompletableNode
import optimus.graph.NodeTask
import optimus.graph.OGSchedulerContext
import optimus.graph.SchedulerPlugin
import optimus.platform._
import optimus.platform.dal.config._

import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

/**
 * Once you comprehend this example, you will understand syncstacks, and syncstack logging In summary (where N
 * designates a node) a => b => c => d => e => g => h => i => j => k => l => m N N N N N N N N f => h => i => j => k =>
 * l => m
 *
 * m() makes a call to an adapted node, so this is a critical syncstack syncstack is where you go async => sync => async code
 *
 * In this example, methods a through m are invoked, calling a,b,c,d,e,f or g causes a sync stack. However, you will
 * only see two invocations where critical syncstacks are logged This is due to deduplication - the key is basically
 * "e,g,h,i,j,k" OR "f,h,i,j,k" Previously only the top 4 of "sync" section was logged, making it sometimes tricky to
 * track down problems ie "h,i,j,k" in this example. Now you get also the complete NodeTrace (tm), including a bit of
 * plugin magic
 *
 * So for a(1) you get the following
 *
 * Stack to be asynced
 * optimus.examples.platform.CriticalSyncStackFoo.k(SyncStackDetectionExample.scala:83)
 * optimus.examples.platform.CriticalSyncStackFoo.j(SyncStackDetectionExample.scala:81)
 * optimus.examples.platform.CriticalSyncStackFoo.i(SyncStackDetectionExample.scala:80)
 * optimus.examples.platform.CriticalSyncStackFoo.h(SyncStackDetectionExample.scala:78)
 * optimus.examples.platform.CriticalSyncStackFoo.g(SyncStackDetectionExample.scala:75)
 * optimus.examples.platform.CriticalSyncStackFoo$$e$node$1.func(SyncStackDetectionExample.scala:73)
 * optimus.examples.platform.CriticalSyncStackFoo$$e$node$1.func(SyncStackDetectionExample.scala:73)
 * optimus.graph.profiled.NodeSync.run(Node.scala:460)
 * Node Trace
 *  0. optimus.examples.platform.CriticalSyncStackFoo.k - (SyncStackDetectionExample.scala:85)SRCi:u
 *
 * paths=1, maxdepth=0
 */

object SyncStackingPlugin extends SchedulerPlugin {
  private val threadBuilder = new ThreadFactoryBuilder().setDaemon(true).build()
  private val executor = Executors.newSingleThreadScheduledExecutor(threadBuilder)
  override def adapt(n: NodeTask, ec: OGSchedulerContext): Boolean = {
    val sc = EvaluationContext.scheduler
    val cb: Runnable = () => n.asInstanceOf[CompletableNode[Boolean]].completeWithResult(true, sc)
    executor.schedule(cb, 1, TimeUnit.SECONDS)
    true
  }
}

@entity
object SyncStacking {
  asyncToSync_info.setPlugin(SyncStackingPlugin)

  @node def asyncToSync(): Boolean = throw new IllegalStateException("must use plugin")
}

@entity
class CriticalSyncStackFoo(id: Int) {
  @node def a(n: Int): Boolean = b(n)
  @node def b(n: Int): Boolean = c(n)
  def c(n: Int): Boolean = d(n)
  def d(n: Int): Boolean = e(n)
  @node def e(n: Int): Boolean = g(n) // note skipping to g (this is syncstack entry point 1)
  @node def f(n: Int): Boolean = h(n) // note skipping to h (this is syncstack entry point 2)
  def g(n: Int): Boolean = h(n)
  def h(n: Int): Boolean = {
    // to highlight that line numbers are for methods, not call points
    i(n)
  }
  def i(n: Int) = j(n): Boolean
  def j(n: Int) = k(n): Boolean
  // docs-snippet:test-breakpoint
  @node def k(n: Int): Boolean = {
    // to highlight that line numbers are for methods, not call points
    l(n)
  }
  // docs-snippet:test-breakpoint
  @node def l(n: Int): Boolean = {
    // to highlight that line numbers are for methods, not call points
    m(n)
  }
  @node def m(n: Int): Boolean = SyncStacking.asyncToSync()

  // Generate seq stack
  @node def n(i: Int): Boolean =
    Seq(i).aseq.map { x =>
      k(x + 1)
    }.head
}

object SyncStackDetectionExample extends OptimusApp[OptimusAppCmdLine] {
  override def dalLocation: DalLocation = DalEnv("dev")

  @entersGraph override def run(): Unit = {
    val foo = CriticalSyncStackFoo(1)
    val calls = Seq(
      foo.a _,
      foo.b _,
      foo.c _,
      foo.d _,
      foo.e _,
      foo.f _,
      foo.g _,
      foo.h _,
      foo.i _,
      foo.j _,
      foo.k _,
      foo.l _,
      foo.m _,
      foo.n _)
    calls.zipWithIndex.map { case (fn, n) =>
      // critical sync stack is also written to stderr, wanted to ensure these labels also appear there
      System.err.println(s"${('a' + n).toChar}(${n + 1})")
      fn(n)
    }
  }
}
