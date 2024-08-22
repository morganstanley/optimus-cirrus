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

import java.util.concurrent.Callable
import java.util.concurrent.CancellationException
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import optimus.platform.annotations.closuresEnterGraph
import optimus.platform.dal.DALEntityResolver
import optimus.platform.dal.config.DalAppId

import java.util.concurrent.ExecutorService
import scala.annotation.tailrec
import scala.util.Try
import scala.util.control.NonFatal

/*
 * Can be used to allow non-optimus threads passing execution requests into optimus ones.
 * Shan't be used for parallel execution inside the optimus itself
 * This trait has a Java-friendly interface.
 */
trait OptimusBgExecutor {

  def runAsync(r: Runnable): Unit
  def runSync(r: Runnable): Unit

  /** Create an asynchronous execution request. All the requests are queued in the same queue */
  def runAsync(f: () => Unit): Unit

  /** Create a synchronous execution request. All the requests are queued in the same queue */
  @closuresEnterGraph def runSync[ResTp](f: => ResTp): ResTp

  /** Create a stop execution request. The service is stopped once all the requests are processed */
  def close(): Unit
}

/*
 * Can be used to allow non-optimus threads passing execution requests into optimus ones.
 * Shan't be used for parallel execution inside the optimus itself
 */
object OptimusService {

  private val logger = msjava.slf4jutils.scalalog.getLogger(this)

  private val serial = new java.util.concurrent.atomic.AtomicInteger(0)

  private class Resident(
      private val id: String,
      private val runtimeEnvironment: RuntimeEnvironment,
      private val isSingleThreadedOptimus: Boolean)
      extends OptimusBgExecutor {

    protected val executor: ExecutorService = Executors.newSingleThreadExecutor((r: Runnable) => {
      val runnable = new Runnable() {
        override def run(): Unit = {
          _thread = Thread.currentThread()
          val nm = _thread.getName
          _thread.setName(nm.substring(0, scala.math.min(nm.length, 32)) + " id:" + id)
          r.run()
        }
      }
      new Thread(runnable)
    })

    @volatile private var _thread: Thread = _

    def onOwnThread: Boolean = Thread.currentThread() eq _thread

    override def close(): Unit = {
      Try { // we shall not throw here
        runSync {
          if (EvaluationContext.isInitialised) {
            EvaluationContext.entityResolver.asInstanceOf[DALEntityResolver].dsi.close(true)
          }
        }
      } recover { case NonFatal(ex) =>
        logger.warn(s"failed to close OptimusBgExecutor $id with error: $ex")
      }
      executor.shutdown()
    }

    def runAsync(r: Runnable): Unit = runTask { r.run() }
    def runSync(r: Runnable): Unit = runSync { r.run() }

    protected def checkState(): Unit =
      if (executor.isShutdown) throw new IllegalStateException(s"$id: Service has been requested to stop")

    protected def ensureInitialized(): Unit = {
      if (!EvaluationContext.isInitialised) {
        if (isSingleThreadedOptimus)
          EvaluationContext.initializeWithNewInitialTimeSingleThreaded(runtimeEnvironment)
        else
          EvaluationContext.initializeWithNewInitialTime(runtimeEnvironment)
      }
      // Note: we move this `EvaluationContext.initializeWithNewInitialTimeSingleThreaded(env)` here so that
      // if any exception during ensureInitializedSingleThreaded (i.e., DAL issue caused GetInfo failures), it
      // could propagate the exception back to the caller as "ExecutionException" when waiting for the result
      // of the FutureTask
    }

    protected def runTask[ResTp](f: => ResTp): Future[ResTp] = {
      checkState()
      val callable = new Callable[ResTp] {
        override def call(): ResTp = {
          ensureInitialized()
          f
        }
      }
      executor.submit(callable)
    }

    def runAsync(f: () => Unit): Unit = {
      runTask[Unit](f())
    }

    /**
     * @throws CancellationException
     *   if the computation was cancelled
     * @throws InterruptedException
     *   if the current thread was interrupted while waiting
     */
    def runSync[ResTp](f: => ResTp): ResTp = {
      require(!onOwnThread, "possible Deadlock")
      val fut = runTask[ResTp](f)
      @tailrec def waitForResult: ResTp = {
        try fut.get(5, TimeUnit.MINUTES)
        catch {
          case _: TimeoutException =>
            logger.info(s"Still waiting for completion of OptimusService runSync call")
            logger.debug(s"Stack trace is: ", new RuntimeException("Waiting stacktrace"))
            waitForResult
          case ex: ExecutionException =>
            // unwrap the cause of the ExecutionException to be returned to the caller
            logger.warn(s"OptimusService runSync call failed to compute", ex)
            val underlying =
              if ((ex.getCause ne null) && ex.getCause != ex) ex.getCause
              else ex
            throw underlying
        }
      }
      waitForResult
    }
  }

  private def nextDefaultId = "Ot#" + serial.incrementAndGet

  /*
   * Java-friendly API, no default args
   */

  // Multi-Threaded Optimus

  // better use createIsolatedBgExecutor instead, we can still use this in test
  def createMtBgExecutor(t: OptimusTask): OptimusBgExecutor = {
    // allegedly this may be used by clients from Java with null value so adding some extra info here
    require(t != null, "Please specify OptimusTask")
    doCreate(t, nextDefaultId, isSingleThreadedOptimus = false)
  }

  // Single-Threaded Optimus

  // better use createIsolatedBgExecutor instead, we can still use this in test
  def createBgExecutor(t: OptimusTask, id: String): OptimusBgExecutor = {
    // allegedly this may be used by clients from Java with null value so adding some extra info here
    require(t != null, "Please either specify OptimusTask or use createBgExecutor(appId, id)")
    doCreate(t, id, isSingleThreadedOptimus = true)
  }

  // better use createIsolatedBgExecutor instead, we can still use this in test
  def createBgExecutor(t: OptimusTask): OptimusBgExecutor = {
    // allegedly this may be used by clients from Java with null value so adding some extra info here
    require(t != null, "Please either specify OptimusTask or use createBgExecutor(appId)")
    doCreate(t, nextDefaultId, isSingleThreadedOptimus = true)
  }

  // better use createIsolatedBgExecutor instead, we can still use this in test
  def createBgExecutor(appId: DalAppId, id: String): OptimusBgExecutor = {
    doCreate(new OptimusTaskCls(appId), id, isSingleThreadedOptimus = true)
  }

  // better use createIsolatedBgExecutor instead, we can still use this in test
  def createBgExecutor(appId: DalAppId): OptimusBgExecutor = {
    doCreate(new OptimusTaskCls(appId), nextDefaultId, isSingleThreadedOptimus = true)
  }

  // 1. when caller thread has an existing EvaluationContext.env, it will reuse it and ignore the task's settings
  // 2. when caller thread has no EvaluationContext.env, it will forceInit the caller thread first, then reuse its env
  private def doCreate(t: OptimusTask, id: String, isSingleThreadedOptimus: Boolean) = {
    val rt = t.withOptimus(() => EvaluationContext.env, finalizationBehavior = OptimusTask.KeepAlive)
    val r = new Resident(id, rt, isSingleThreadedOptimus)
    r
  }

  // unlike createBgExecutor's behavior, this method will encapsulate task's settings within BgExecutor managed thread(s):
  // - it will always honor the task's setting to setup the BgExecutor
  // - it will not do anything to the caller thread
  def createIsolatedBgExecutor(
      t: OptimusTask,
      id: String = nextDefaultId,
      isSingleThreadedOptimus: Boolean = true): OptimusBgExecutor = {
    val components = t.createRuntimeComponents()
    val env = components.createRuntime()
    new Resident(id, env, isSingleThreadedOptimus) {
      override protected def ensureInitialized(): Unit = {
        if (!EvaluationContext.isInitialised) {
          try {
            if (isSingleThreadedOptimus)
              EvaluationContext.initializeWithNewInitialTimeSingleThreaded(env)
            else
              EvaluationContext.initializeWithNewInitialTime(env)
          } catch {
            case NonFatal(ex) =>
              if (env.entityResolver ne null)
                env.entityResolver.close()
              throw ex
          }
        }
      }
    }
  }
}
