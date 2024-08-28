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

import com.sun.management.GarbageCollectionNotificationInfo
import javax.management.Notification
import javax.management.NotificationListener
import javax.management.openmbean.CompositeData
import optimus.graph.diagnostics.messages.GCCounter
import optimus.graph.diagnostics.messages.GCEvent
import optimus.graph.diagnostics.messages.HeapMemoryCounter

/**
 * GC Listener that publishes events to OGTrace that are read back and rendered by the NodeTimeLineView
 */
class OGTraceGCListener extends NotificationListener {

  override def handleNotification(notification: Notification, handback: Any): Unit = {
    notification.getType match {
      case GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION =>
        val notifInfo = GarbageCollectionNotificationInfo.from(notification.getUserData.asInstanceOf[CompositeData])

        val gcInfo = notifInfo.getGcInfo
        val gcCause = notifInfo.getGcCause

        if (OGTraceIsolation.isTracing) {
          val gcAction = notifInfo.getGcAction
          val eventType =
            if (gcAction.contains("minor GC")) GCEvent.minor
            else if (gcAction.contains("major GC")) GCEvent.major
            else GCEvent.other

          var usageBefore = 0L
          var usageAfter = 0L
          gcInfo.getMemoryUsageBeforeGc.forEach((_, memory) => { usageBefore += memory.getUsed })
          gcInfo.getMemoryUsageAfterGc.forEach((_, memory) => { usageAfter += memory.getUsed })
          HeapMemoryCounter.reportCurrent()
          GCCounter.report(eventType, gcCause, gcInfo.getStartTime, gcInfo.getEndTime, usageBefore - usageAfter)
        }
    }
  }
}
