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
package optimus.dsi.base.watchtower

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import msjava.base.event.publication.watchtower.SimpleWatchTowerEvent
import msjava.base.event.publication.watchtower.WatchTowerEvent
import msjava.slf4jutils.scalalog.getLogger
import scala.jdk.CollectionConverters._

final case class PublisherIdentity(resource: String, facet: String)

trait WatchtowerPublisher {
  def fire(publisher: PublisherIdentity, e: WatchTowerEvent): Unit
  def flush(): Unit
  def flush(publisher: PublisherIdentity): Unit
  def addPreShutdownHook(hook: () => Unit): Unit
  private[watchtower] def shutdown: Unit
}

class SingleResourceWatchtowerPublisher(resource: String, facet: String, publisher: WatchtowerPublisher) {
  private val publisherId = PublisherIdentity(resource, facet)
  def fire(e: WatchTowerEvent) = publisher.fire(publisherId, e)
  def flush() = publisher.flush(publisherId)
}

trait DalWatchTowerPublisherLike {
  def sendEvent(eventType: DalWatchTowerEvent): Unit
}

/**
 * Singleton instance should be sufficient for all DAL (broker) related events. This internally uses global
 * WatchTowerPublisher implementation present in utils.
 */
object DalWatchTowerPublisher extends DalWatchTowerPublisherLike {

  private[this] val log = getLogger[DalWatchTowerPublisher.type]

  final case class WatchTowerPublisherInstance(
      publisher: SingleResourceWatchtowerPublisher,
      dalEnv: String,
      instanceName: String,
      hostPort: String)

  // Singleton instance for publishing watch tower events..
  private[optimus] val singletonPublisher = new AtomicReference[WatchTowerPublisherInstance]

  private lazy val titlePrefix: String = Option(singletonPublisher.get) match {
    case Some(instance) => s"${instance.dalEnv} ${instance.hostPort}"
    case None           => ""
  }

  private def createEvent(eventType: DalWatchTowerEvent, instanceName: String): WatchTowerEvent = {
    val payload = eventType.payload(titlePrefix).asJava
    new SimpleWatchTowerEvent(UUID.randomUUID.toString, s"${eventType.id}-$instanceName", payload, instanceName)
  }

  def sendEvent(eventType: DalWatchTowerEvent): Unit = {
    Option(singletonPublisher.get) match {
      case Some(instance) =>
        val event = createEvent(eventType, instance.instanceName)

        // We're synchronously sending because events in DAL broker are triggered in exceptional
        // cases. And we want these events to be published right away..
        instance.publisher.fire(event)
      case None =>
        log.warn(s"WatchTower event publisher instance has not been created. Won't publish event: $eventType")
    }
  }
}
