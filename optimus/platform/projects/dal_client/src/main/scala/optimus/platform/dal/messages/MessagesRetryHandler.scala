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
package optimus.platform.dal.messages

import msjava.slf4jutils.scalalog.getLogger
import optimus.dsi.pubsub.CustomThreadGroup
import optimus.platform.dal.pubsub.NotificationStreamState
import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.protobufutils.DalBrokerClient

import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic._
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutorService
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

object MessagesRetryHandler {
  private val retryHandlerThreadPoolCount = new AtomicInteger(0)
  private lazy val retryThreadGroup = new CustomThreadGroup("MessagesRetryHandler")
}

trait MessagesRetryHandler { proxy: MessagesDsiProxyBase =>

  import MessagesRetryHandler._
  private val log = getLogger(this)

  private[messages] def getClient: MessagesClient

  protected def withRetry[T](cmds: Seq[MessagesStreamCommand])(f: MessagesClient => T): Option[T] = {
    val brokerClient =
      try {
        Some(getClient)
      } catch {
        case th: Throwable =>
          handleMessagesFailure(None, th)
          None
      }
    try {
      brokerClient match {
        case Some(b) =>
          val areAllCreateCmds = cmds.forall(_.isInstanceOf[CreateMessagesStream])
          if (areAllCreateCmds) {
            val output = Some(f(b))
            lastKnownGoodBrokerClient.set(b)
            output
          } else {
            // Means ChangeSubscription commands are present. Make sure that
            // we execute those commands only if client is the same that created those streams.
            // Else it is possible that retry is happening due to a transient issue, and we
            // simply trigger yet another retry.
            Option(lastKnownGoodBrokerClient.get) match {
              case Some(previousClient) if previousClient == b => Some(f(b))
              case mayBePreviousClient =>
                handleMessagesFailure(
                  mayBePreviousClient,
                  new DALRetryableActionException(
                    s"Retrying ${cmds.size} non-create command(s) because previous client $mayBePreviousClient is not the same as current client $b",
                    mayBePreviousClient)
                )
                None
            }
          }
        case None => None
      }
    } catch {
      case retryable: DALRetryableActionException =>
        handleMessagesFailure(brokerClient, retryable)
        None
      case th: Throwable =>
        // we are making sure that ongoing streams aren't affected by non-retryable errors for a single stream
        cmds foreach { messagesCmd =>
          val error = messagesCmd match {
            case cms: CreateMessagesStream => Some(new MessagesStreamCreationException(cms.streamId, th.getMessage))
            case cms: CloseMessagesStream  => Some(new MessagesStreamClosureException(cms.streamId, th.getMessage))
            case subs: ChangeMessagesSubscription =>
              Some(new MessagesSubscriptionChangeException(subs.streamId, subs.changeRequestId, th.getMessage))
            case _: CommitMessagesStream =>
              // We can ignore as this command is treated as best effort command.
              None
            case cls: CreateMessagesClientStream => throw new IllegalArgumentException(s"Do not expect $cls command!")
          }
          // given the exception is not retryable, passing brokerClient as None, just to make it explicit that
          // the current client shouldn't be closed in any way
          error.foreach(handleMessagesFailure(None, _))
        }
        None
    }
  }

  // We keep an instance of last known good client to make sure that commands like ChangeMessagesSubscription/CloseMessagesStream
  // are sent using the same "client" (i.e., to same broker host) which created those streams. This way we avoid the
  // edge condition where due to some transient issue streams are getting reconnected, but before that happens
  // ChangeMessagesSubscription/CloseMessagesStream commands are sent to another broker, which doesn't have corresponding streams
  // yet and fails.
  protected[this] val lastKnownGoodBrokerClient = new AtomicReference[MessagesClient]()

  protected def handleMessagesFailure(failedClient: Option[DalBrokerClient], th: Throwable): Unit = {
    if (getCurrentStreamObjs.nonEmpty) {
      if (th.getCause.isInstanceOf[MessagesStreamDisconnectException]) {
        onDisconnect()
      }
      th match {
        case rt: DALRetryableActionException =>
          handleMessagesRetry(failedClient, rt)
        case _: Throwable =>
          // we won't retry it here
          sendExceptionUpstream(th)
      }
    }
  }

  protected[messages] val retryCount = new AtomicInteger(0)

  protected def generateStreamRetryCmds(rt: DALRetryableActionException): Seq[(CreateMessagesStream)] = {
    getCurrentStreamObjs.flatMap { stream =>
      if (stream.getState != NotificationStreamState.OngoingRefresh && retryCount.get > 1)
        retryCount.set(0)
      val cmdOpt = stream.refreshStream(extractRtCause(rt))
      if (cmdOpt.isEmpty) {
        // it means that we don't want to retry on this stream (either pending closure state or run out of retries)
        sendErrorAndCloseStream(stream.id, extractRtCause(rt))
        None
      } else cmdOpt
    }
  }

  protected def getCurrentStreamObjs: Seq[MessagesNotificationStream] =
    proxy.streamMap.asScala.values.toSeq

  protected def onDisconnect(): Unit =
    getCurrentStreamObjs.foreach { stream => stream.onDisconnect() }

  protected def sendErrorAndCloseStream(streamId: String, th: Throwable): Unit = {
    val streamObj = proxy.streamMap.asScala.get(streamId)
    streamObj.foreach(stream => stream.shutdown(Some(th)))
  }

  protected def closeStreamManager(): Unit = {
    retryHandlerThreadPool.shutdownNow()
    shutdownAllStreams()
  }

  private[messages] def shutdownAllStreams(): Unit = {
    getCurrentStreamObjs.foreach { stream =>
      stream.shutdown()
    }
    proxy.streamMap.clear()
  }

  protected def onConnect(): Unit =
    getCurrentStreamObjs.foreach { stream => stream.onConnect() }

  private lazy val retryHandlerThreadPool = {
    val factory = new ThreadFactory {
      override def newThread(runnable: Runnable): Thread = {
        val thread =
          new Thread(retryThreadGroup, runnable, s"MessagesRetryHandler-${retryHandlerThreadPoolCount.incrementAndGet}")
        thread.setDaemon(true)
        thread
      }
    }
    Executors.newSingleThreadExecutor(factory)
  }
  private lazy val retryHandlerExecutionContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(retryHandlerThreadPool)

  protected def extractRtCause(rt: DALRetryableActionException): Throwable = Option(rt.getCause).getOrElse(rt)

  protected def sendExceptionUpstream(th: Throwable): Unit = {
    val errorResults = th match {
      case mse: MessagesStreamException if !mse.isInstanceOf[MessagesTransientException] =>
        Seq(ErrorResult(mse))
      case ex =>
        // we will broadcast a transient exception to all the streams, in case we have run out of retries.
        getCurrentStreamObjs.map { stream =>
          ErrorResult(new MessagesTransientException(stream.id, ex.getMessage))
        }
    }
    proxy.callback.handleResults(errorResults)
  }

  // Keeping the state to avoid recursive retry..
  val allowRetry = new AtomicBoolean(true)

  private def doRetry(rt: DALRetryableActionException): Unit = {
    if (retryCount.incrementAndGet() < proxy.getRetryAttempts) {

      // We close current proto client, if any, and start afresh. This might incur unnecessary overhead in some
      // scenarios, but those are very rare, so, simplifying the logic. Note that it could retry recursively which
      // need to be avoided.
      allowRetry.set(false)
      proxy.closeCurrentSender()
      allowRetry.set(true)

      val backoff = calculateBackoffTimeMs(proxy.getRetryBackoffTime, retryCount.get)
      val cmdsToBeRetried = generateStreamRetryCmds(rt)

      if (cmdsToBeRetried.nonEmpty) {
        log.info(s"Retrying (count#${retryCount.get}) in $backoff millis due to cause: ${extractRtCause(rt)} ")
        Thread.sleep(backoff)
        cmdsToBeRetried.foreach(cmd => proxy.createNewStream(cmd, cmd.subs.map(_.eventClassName)))
      } else {
        // there are no streams to be retried, so send the error upstream
        val cause = extractRtCause(rt)
        sendExceptionUpstream(cause)
      }
    } else if (retryCount.getAndIncrement() == proxy.getRetryAttempts) {
      log.warn(s"Run out of retries, giving up: $rt")
      val cause = extractRtCause(rt)
      sendExceptionUpstream(cause)
    }
  }

  protected def handleMessagesRetry(failedClient: Option[DalBrokerClient], rt: DALRetryableActionException): Unit = {
    if (allowRetry.get) {
      Future {
        try {
          proxy.closeSender(failedClient)
          doRetry(rt)
        } catch {
          case _: InterruptedException =>
            log.info("Retry interrupted, probably runtime is shutting down")
            None
          case ex: Throwable =>
            log.error(s"retry failed.", ex)
            sendExceptionUpstream(ex)
            None
        }
      }(retryHandlerExecutionContext)
    }
  }
}
