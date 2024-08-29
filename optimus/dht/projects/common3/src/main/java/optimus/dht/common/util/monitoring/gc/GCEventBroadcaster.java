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
package optimus.dht.common.util.monitoring.gc;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;

public class GCEventBroadcaster {

  public static GCEventBroadcaster INSTANCE = new GCEventBroadcaster();

  private static final Logger logger = LoggerFactory.getLogger(GCEventBroadcaster.class);

  protected static final String CAUSE_NO_GC = "No GC";
  protected static final String CONCURRENT_PAUSES_NAME_SUFFIX = " Pauses";
  protected static final String ACTION_END_OF_MAJOR_GC = "end of major GC";

  protected static final String SHENANDOAH_CYCLES_NAME = "Shenandoah Cycles";
  protected static final String ZGC_CYCLES_NAME = "ZGC Cycles";
  protected static final String ZGC_MAJOR_CYCLES_NAME = "ZGC Major Cycles";

  protected final NotificationListener internalListener;

  protected final Set<GCEventListener> listeners = new CopyOnWriteArraySet<>();

  protected class InternalGCEventListener implements NotificationListener {
    @Override
    public void handleNotification(Notification notification, Object handback) {
      if (GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION.equals(
          notification.getType())) {
        process(GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData()));
      }
    }
  }

  static {
    // install the default INSTANCE by default - it does not cause any harm
    // people can still create and use their custom subclasses if they want to
    INSTANCE.install();
  }

  protected GCEventBroadcaster() {
    internalListener = createInternalListener();
  }

  protected NotificationListener createInternalListener() {
    return new InternalGCEventListener();
  }

  public void install() {
    ManagementFactory.getGarbageCollectorMXBeans().forEach(bean -> installOnBean(bean));
  }

  public void uninstall() {
    ManagementFactory.getGarbageCollectorMXBeans().forEach(bean -> uninstallOnBean(bean));
  }

  protected void installOnBean(GarbageCollectorMXBean bean) {
    logger.info("Installing GCEventsBroadcaster on {}", bean.getName());
    if (bean instanceof NotificationEmitter) {
      ((NotificationEmitter) bean).addNotificationListener(internalListener, null, bean);
    }
  }

  protected void uninstallOnBean(GarbageCollectorMXBean bean) {
    logger.info("Uninstalling GCEventsBroadcaster from {}", bean.getName());
    if (bean instanceof NotificationEmitter) {
      try {
        ((NotificationEmitter) bean).removeNotificationListener(internalListener, null, bean);
      } catch (ListenerNotFoundException e) {
        logger.warn("Monitor was not installed", e);
      }
    }
  }

  protected void process(GarbageCollectionNotificationInfo info) {

    if (info.getGcName().endsWith(CONCURRENT_PAUSES_NAME_SUFFIX)) {
      // ZGC/Shenandoah pause
      for (GCEventListener listener : listeners) {
        try {
          listener.onGcPause(info, info.getGcInfo());
        } catch (Exception e) {
          logger.warn("GC listener " + listener + " threw an exception", e);
        }
      }

      return;
    }

    if (CAUSE_NO_GC.equals(info.getGcCause())) {
      // concurrent CMS cycle
      return;
    }

    GcInfo gcInfo = info.getGcInfo();
    String gcName = info.getGcName();
    GCMemoryPoolSizes beforeGc = GCMemoryPoolSizes.fromMap(gcInfo.getMemoryUsageBeforeGc());
    GCMemoryPoolSizes afterGc = GCMemoryPoolSizes.fromMap(gcInfo.getMemoryUsageAfterGc());

    if (beforeGc.getHeapUsed() == 0 && afterGc.getHeapUsed() == 0) {
      // other kind of concurrent cycle that wasn't handled above
      return;
    }

    boolean isMajorGc =
        ZGC_MAJOR_CYCLES_NAME.equals(gcName)
            || ZGC_CYCLES_NAME.equals(gcName)
            || SHENANDOAH_CYCLES_NAME.equals(gcName)
            || ACTION_END_OF_MAJOR_GC.equals(info.getGcAction());

    for (GCEventListener listener : listeners) {
      try {
        listener.onGcEvent(info, gcInfo, beforeGc, afterGc, isMajorGc);
      } catch (Exception e) {
        logger.warn("GC listener " + listener + " threw an exception", e);
      }
    }
  }

  public void addListener(GCEventListener listener) {
    listeners.add(listener);
  }

  public void removeListener(GCEventListener listener) {
    listeners.remove(listener);
  }
}
