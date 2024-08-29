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
package optimus.platform.dal.pubsub

import msjava.slf4jutils.scalalog.getLogger
import optimus.dsi.notification.NotificationEntry
import optimus.dsi.pubsub.Subscription
import optimus.platform.ValidTimeInterval
import optimus.platform.dsi.bitemporal._
import optimus.platform.storable.PersistentEntity

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class SubscriptionStateManager(initialSubs: Seq[Subscription]) {
  import SubscriptionStateManager._
  private[this] val presentSubscriptions = new ConcurrentHashMap[Subscription.Id, Subscription](initialSubs.size)
  private[dal] val pendingChanges = new ConcurrentHashMap[Int, SubscriptionChangeInfo]()

  // Note that following data structures can have mutable state inside it as method(s) that manipulate
  // them are synchronized.
  private[dal] val pendingSow = new ConcurrentHashMap[Subscription.Id, SowInformation]()
  private[dal] val pendingDataNotifications = new AtomicReference[Option[PartialDataNotification]](None)

  def currentSubscriptions(): Set[Subscription] = presentSubscriptions.asScala.map { case (_, v) => v }.toSet

  def getSubscription(subId: Subscription.Id): Option[Subscription] = Option(presentSubscriptions.get(subId))

  def sowQueryCompleted(): Boolean = pendingSow.asScala.forall { case (_, v) =>
    v.queryResultOpt.isDefined
  }

  def resetStateManager(postRefresh: Boolean): Unit = {
    presentSubscriptions.clear()
    pendingChanges.clear()
    pendingDataNotifications.set(None)
    if (postRefresh) {
      pendingSow.asScala.foreach {
        case (_, sowInfo) if sowInfo.queryResultOpt.isEmpty => pendingSow.remove(sowInfo.subId)
        case _                                              => // do nothing since we want to retain sow completed subs
      }
    } else {
      pendingSow.clear()
    }
  }

  def subscriptionChangeRequest(changeRequest: ChangeSubscription): Unit = {
    val change =
      SubscriptionChangeInfo(changeRequest.changeRequestId, changeRequest.newSubs.toSet, changeRequest.removeSubs.toSet)
    pendingChanges.putIfAbsent(changeRequest.changeRequestId, change)
  }

  private def handleSowChanges(changeResult: ChangeSubscriptionSuccessResult): Unit = {
    Option(pendingChanges.remove(changeResult.changeRequestId)).foreach { changeInfo =>
      changeInfo.toAdd.foreach { addSub =>
        presentSubscriptions.put(addSub.subId, addSub)
      }
      changeInfo.toRemove.foreach { removeId =>
        presentSubscriptions.remove(removeId)
      }
      addSowRequest(changeInfo.toAdd.toSeq)
      removeSowRequest(changeInfo.toRemove)
    }
  }

  def subscriptionChangeResponse(result: Result, retryCmdOpt: Option[CreatePubSubStream] = None): Unit = {
    result match {
      case _: CreatePubSubStreamSuccessResult if retryCmdOpt.isDefined =>
        // this means that we have re-connected after a refresh
        retryCmdOpt.get.subs.foreach { toAdd =>
          presentSubscriptions.put(toAdd.subId, toAdd)
        }
        addSowRequest(retryCmdOpt.get.subs)
      case _: CreatePubSubStreamSuccessResult =>
        initialSubs.foreach { toAdd =>
          presentSubscriptions.put(toAdd.subId, toAdd)
        }
        addSowRequest(initialSubs)
      case res: ChangeSubscriptionSuccessResult =>
        handleSowChanges(res)
      case ErrorResult(th: PubSubSubscriptionChangeException, _) =>
        val change = pendingChanges.remove(th.changeRequestId)
        removeSowRequest(change.toAdd.map(_.subId))
      case _ => log.error(s"Unexpected result received for subscriptionChangeResponse: $result")
    }

  }

  /**
   * This method handles all the sowResponses. Initially it updates the pendingSow map through updateSowMap() call, and
   * then if all the sow responses are present for this particular stream, then it returns the completed sowMap
   */
  def sowResponse(res: PubSubSowResult): Map[Subscription, Seq[PersistentEntity]] = {
    updateSowMap(res)
    if (sowQueryCompleted()) {
      pendingSow.asScala.collect {
        case (subId, _) if presentSubscriptions.containsKey(subId) =>
          val sowInfo = pendingSow.remove(subId)
          val pes = extractPes(sowInfo.queryResultOpt.get)
          presentSubscriptions.get(subId) -> pes
      }.toMap
    } else {
      Map.empty
    }
  }

  def dataResponse(
      res: DirectPubSubNotificationResult
  ): Map[Subscription, Seq[NotificationEntry]] = {
    require(pendingDataNotifications.get.isEmpty, s"non-empty pending partial data notifications not expected.")
    res.entryMap.collect {
      case (subId, nes) if presentSubscriptions.containsKey(subId) =>
        presentSubscriptions.get(subId) -> nes
    }
  }

  def partialDataResponse(
      res: DirectPubSubNotificationPartialResult
  ): Option[Map[Subscription, Seq[NotificationEntry]]] = {
    val entries = res.entries.filter(entry => presentSubscriptions.containsKey(entry.subId))
    pendingDataNotifications.get match {
      case Some(notifications) =>
        require(
          res.txTime == notifications.txTime,
          s"Expect partial notification for the same tt - (incoming) ${res.txTime} != (current) ${notifications.txTime}")
        notifications.data ++= entries
        if (res.isLast) {
          val fullResult = res.generateFullResult(notifications.data)
          pendingDataNotifications.set(None)
          Some(dataResponse(fullResult))
        } else None
      case None =>
        val notifications = PartialDataNotification(res.txTime)
        notifications.data ++= entries
        pendingDataNotifications.set(Some(notifications))
        None
    }
  }

  private def addSowRequest(subs: Seq[Subscription]): Unit = {
    subs.foreach {
      case toAdd if toAdd.includeSow =>
        pendingSow.putIfAbsent(toAdd.subId, SowInformation(toAdd.subId))
      case _ => // do nothing here since includeSow is false
    }
  }

  private def removeSowRequest(toRemove: Set[Subscription.Id]): Unit = {
    toRemove.foreach { subId =>
      pendingSow.remove(subId)
    }
  }

  /**
   * This method will update the pendingSow map with the received results. The queryResultOpt in the SowInformation
   * class contains the entire QueryResult of the sow query. If this is defined, it means that the sow is complete for
   * the particular subscription.
   */
  private def updateSowMap(res: PubSubSowResult): Unit = {
    Option(pendingSow.get(res.subId)).foreach { existingSowInfo =>
      res match {
        case PubSubSowFullResult(_, subId, _, result) =>
          pendingSow.replace(subId, existingSowInfo.copy(queryResultOpt = Some(result)))
          log.debug(s"SOW completed for subscriptionId: $subId")

        case PubSubSowPartialResult(_, subId, _, partialQueryResult) =>
          val updatedBuffer = existingSowInfo.tempBuffer ++ partialQueryResult.values
          if (partialQueryResult.isLast) {
            val queryResOpt = Some(
              partialQueryResult
                .generateFullResult(updatedBuffer, partialQueryResult.metaData.get)
                .asInstanceOf[QueryResult])
            pendingSow.replace(
              subId,
              existingSowInfo.copy(tempBuffer = mutable.ArrayBuffer.empty, queryResultOpt = queryResOpt))
            log.debug(s"SOW completed for subscriptionId: $subId")
          } else {
            pendingSow.replace(subId, existingSowInfo.copy(tempBuffer = updatedBuffer))
          }
      }
    }
  }

  private def extractPes(result: QueryResult): Seq[PersistentEntity] = {
    require(result.metaData.peFields.size == 1, "We expect only 1 PersistentEntity column")
    val peFieldIdx = result.metaData.peFields.head
    result.value.map { data =>
      data(peFieldIdx).asInstanceOf[PersistentEntity]
    }.toSeq
  }

  /**
   * This method will generate the necessary CreateStream command to re-establish the stream onto a new broker, in the
   * event of a retry. During the creation of this new command, we take care of the following:
   *   - presentSubscriptions: we add all these to the new command
   *   - pendingAdds: we add these too in the new command
   *   - pendingRemoves: we remove these from the new command, thereby creating a "net" addPubSub command
   *
   * Regarding the sow states, we avoid send sow queries for the subscriptions which have already received their sow
   * responses. These responses can either be present in the pendingSow map or they might already have been notified to
   * the client.
   */
  def createRetryAddPubSubCmd(
      streamId: String,
      startTime: Option[Instant],
      endTime: Option[Instant],
      streamInitialized: Boolean,
      vtFilterInterval: Option[ValidTimeInterval]
  ): CreatePubSubStream = {
    val subs = if (streamInitialized) {
      val pendingAdds = pendingChanges.asScala.flatMap { case (_, v) => v.toAdd }
      val pendingRemoves = pendingChanges.asScala.flatMap { case (_, changeInfo) =>
        changeInfo.toRemove.collect {
          case subId if presentSubscriptions.containsKey(subId) =>
            presentSubscriptions.get(subId)
        }
      }
      val subsToBeAdded = presentSubscriptions.asScala.map { case (_, v) => v }.toSet ++ pendingAdds -- pendingRemoves

      subsToBeAdded.map {
        case sub
            if pendingSow.containsKey(sub.subId) && pendingSow
              .get(sub.subId)
              .queryResultOpt
              .isDefined =>
          // we don't want sow to be repeated, since, the queryResultOpt is already present
          sub.copy(includeSow = false)
        case sub if sub.includeSow && !pendingSow.containsKey(sub.subId) =>
          // these are the older subs whose sow queries have already been completed
          sub.copy(includeSow = false)
        case sub => sub
      }
    } else {
      initialSubs
    }
    CreatePubSubStream(streamId, subs.toSeq, startTime, endTime, vtFilterInterval)
  }

}

object SubscriptionStateManager {
  private val log = getLogger(this)

  /**
   * @param subId
   *   SubscriptionId
   * @param tempBuffer
   *   Buffer to hold the PartialQueryResult of the SowResponse
   * @param queryResultOpt
   *   If defined, it will contain the complete sowResult
   */
  final case class SowInformation(
      subId: Subscription.Id,
      tempBuffer: mutable.ArrayBuffer[Array[Any]] = mutable.ArrayBuffer.empty,
      queryResultOpt: Option[QueryResult] = None)

  final case class SubscriptionChangeInfo(changeId: Int, toAdd: Set[Subscription], toRemove: Set[Subscription.Id])

  final case class PartialDataNotification(
      txTime: Instant,
      data: mutable.ArrayBuffer[PubSubNotificationResult.SimpleEntry] = new mutable.ArrayBuffer())
}
