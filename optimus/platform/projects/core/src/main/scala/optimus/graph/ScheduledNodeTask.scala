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
package optimus.graph

import optimus.platform.ScenarioStack

import java.util.concurrent.{Executors, ScheduledExecutorService, ScheduledThreadPoolExecutor, ThreadFactory, TimeUnit}

private[graph] abstract class ScheduledNodeTask(private val scheduler: Scheduler) extends NodeTask with Runnable {
  override def executionInfo(): NodeTaskInfo = NodeTaskInfo.Scheduled

  // Implementation for Runnable
  final override def run(): Unit = {
    scheduler.enqueue(this)
  }

  final def schedule(delay: Long, unit: TimeUnit): Unit = {
    ScheduledNodeTask.schedule(this, delay, unit)
  }
}

object ScheduledNodeTask {

  /**
   * Create a scheduled NodeTask by wrapping a Runnable.
   *
   * The runnable will run in a no-caching scenario stack.
   */
  def fromRunnable(scheduler: Scheduler, r: Runnable): ScheduledNodeTask = new ScheduledNodeTask(scheduler) {
    attach(ScenarioStack.constantNC)

    override def run(ec: OGSchedulerContext): Unit = {
      r.run()
      complete(ec)
    }
  }

  /**
   * A scheduled executor which exists only to give tasks to optimus schedulers.
   *
   * Unlike the executor in CoreAPI, this one doesn't run anything but NodeTask enqueues.
   */
  private[this] val executor: ScheduledExecutorService = {
    val pool = new ScheduledThreadPoolExecutor(
      1,
      new ThreadFactory {
        override def newThread(r: Runnable): Thread = {
          val thread = Executors.defaultThreadFactory.newThread(r)
          thread.setName(s"TimedNodeTask")
          thread.setDaemon(true)
          thread
        }
      }
    )
    pool.setKeepAliveTime(1, TimeUnit.MINUTES)
    pool.allowCoreThreadTimeOut(true)
    pool.setRemoveOnCancelPolicy(true)
    pool
  }

  /**
   * Run a Runnable on a delay as a node task.
   */
  private def schedule(task: ScheduledNodeTask, delay: Long, unit: TimeUnit): Unit = {
    executor.schedule(task, delay, unit)
  }
}
