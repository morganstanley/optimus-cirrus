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
package optimus.platform.dal.client.prc

import com.ms.silverking.cloud.dht.client.AsyncOperation
import com.ms.silverking.cloud.dht.client.AsyncOperationListener
import com.ms.silverking.cloud.dht.client.AsyncRetrieval
import com.ms.silverking.cloud.dht.client.FailureCause
import com.ms.silverking.cloud.dht.client.OperationState
import optimus.platform.ImmutableArray
import optimus.platform.dal.prc.PrcExecutionException
import optimus.platform.dsi.bitemporal.DALRetryableActionException
import optimus.platform.dsi.bitemporal.proto.Prc.PrcSingleKeyResponseProto
import optimus.platform.dsi.prc.cache.NonTemporalPrcKeyUserOpts
import optimus.platform.dsi.protobufutils.ProtoBufUtils

import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Try

object PrcAsyncOperationListener {
  private def executionException(cause: FailureCause) = new PrcExecutionException(s"PRC execution failed: $cause")
  private def retryableException(cause: FailureCause) =
    new DALRetryableActionException(s"PRC failed with retryable cause $cause", None)

  def exceptionForCause(failureCause: FailureCause): Throwable = {
    failureCause match {
      case FailureCause.ERROR | FailureCause.MUTATION | FailureCause.MULTIPLE | FailureCause.INVALID_VERSION |
          FailureCause.SIMULTANEOUS_PUT | FailureCause.NO_SUCH_VALUE | FailureCause.NO_SUCH_NAMESPACE |
          FailureCause.CORRUPT | FailureCause.LOCKED =>
        executionException(failureCause)
      case FailureCause.SESSION_CLOSED | FailureCause.ALL_REPLICAS_EXCLUDED | FailureCause.TIMEOUT =>
        retryableException(failureCause)
    }
  }

  def retryableFor(t: Throwable): Throwable = {
    new DALRetryableActionException(s"PRC failed with retryable exception", t, None)
  }
}

class PrcAsyncOperationListener(
    seqId: Int,
    messageReceiver: PrcMessageReceiver,
    cmdIndexesByKey: Map[ImmutableArray[Byte], Seq[Int]],
    prcKeyUserOptsByCmdIndex: Map[Int, NonTemporalPrcKeyUserOpts],
    redirectionCache: PrcRedirectionCache)
    extends AsyncOperationListener {

  import PrcAsyncOperationListener.exceptionForCause

  private var keysCompleted = 0
  def expectingFurtherResults(): Boolean = keysCompleted < cmdIndexesByKey.size
  def getKeysCompleted: Int = keysCompleted

  protected def parseResponse(v: Array[Byte]): PrcSingleKeyResponseProto = ???
    /* PrcSingleKeyResponseProto.parseFrom(ProtoBufUtils.getCodedInputStream(v)) */

  private def onKeyFinished(retrieval: AsyncRetrieval[Array[Byte], Array[Byte]]): Unit = ??? /* synchronized {
    val storedValues = retrieval.getLatestStoredValues.asScala
    checkUpdateExpected(storedValues.size)
    storedValues.foreach { case (key, storedValue) =>
      keysCompleted += 1

      val response = Try { parseResponse(storedValue.getValue) }

      val cmdIndexesForKey = cmdIndexesByKey
        .getOrElse(
          ImmutableArray.wrapped(key),
          throw new IllegalArgumentException(s"Could not complete operation for untracked key: $key"))
      val isPartial = expectingFurtherResults()

      response.toOption.foreach { r =>
        val resultsWithIndices = r.getResultsList.asScala.zip(cmdIndexesForKey)
        resultsWithIndices.foreach { case (resultProto, index) =>
          val userOpts = prcKeyUserOptsByCmdIndex
            .getOrElse(index, throw new IllegalArgumentException(s"No userOpts available at index $index"))
          redirectionCache.update(userOpts, resultProto)
        }
      }

      messageReceiver.messageCallback(
        DalPrcResponseMessage(
          seqId,
          response.map { res =>
            PrcResponseWrapper(isPartial, res, cmdIndexesForKey)
          }))
    }
  } */

  override def asyncOperationUpdated(asyncOperation: AsyncOperation): Unit = {
    require(
      asyncOperation.isInstanceOf[AsyncRetrieval[_, _]],
      s"PRC expected an SK AsyncRetrieval but was: $asyncOperation")
    val retrieval = asyncOperation.asInstanceOf[AsyncRetrieval[Array[Byte], Array[Byte]]]
    val state = retrieval.getState
    state match {
      case OperationState.FAILED =>
        if (!expectingFurtherResults()) {
          throw new IllegalStateException(
            s"Async op already ended with a Failure, but we got further updates ($keysCompleted/${cmdIndexesByKey.size} completed keys)")
        }
        val failureCause = retrieval.getFailureCause
        notifyOpFailed(failureCause)
        // OpState.Failed ends the asyncOp, so we can count every key as completed now.
        keysCompleted = cmdIndexesByKey.size
        messageReceiver.messageCallback(DalPrcResponseMessage(seqId, Failure(exceptionForCause(failureCause))))
      case OperationState.INCOMPLETE =>
        // Incomplete implies further updates will arrive in the future
        onKeyFinished(retrieval)
        notifyOpUpdatedButNotComplete()
      case OperationState.SUCCEEDED =>
        onKeyFinished(retrieval)
        notifyOpSucceeded()
        // Succeed ends the AsyncOp. If we were expecting further results,
        // an Incomplete ought have arrived rather than a Succeeded
        if (expectingFurtherResults())
          throw new IllegalStateException(
            s"Async op ended with Success, but we expected more results - got $keysCompleted/${cmdIndexesByKey.size}")
    }
  }

  private def checkUpdateExpected(newKeys: Int): Unit = {
    if (!expectingFurtherResults())
      throw new IllegalStateException(
        s"SilverKing returned more keys than expected: requested ${cmdIndexesByKey.size}, got ${keysCompleted + newKeys}")
  }

  // Test Hooks
  protected def notifyOpFailed(cause: FailureCause): Unit = {}
  protected def notifyOpSucceeded(): Unit = {}
  protected def notifyOpUpdatedButNotComplete(): Unit = {}

}
