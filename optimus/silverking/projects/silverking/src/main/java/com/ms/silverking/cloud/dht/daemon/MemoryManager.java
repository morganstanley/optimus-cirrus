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
package com.ms.silverking.cloud.dht.daemon;

import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.JVMUtil;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.memory.JVMMemoryObserver;
import com.ms.silverking.util.memory.JVMMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Watches over JVM heap usage. */
public class MemoryManager implements JVMMemoryObserver {
  private final JVMMonitor monitor;

  private static final int jvmMonitorMinIntervalMillis =
      PropertiesHelper.systemHelper.getInt(
          DHTConstants.jvmMonitorMinIntervalMillisProperty, 10 * 1000);
  private static final int jvmMonitorMaxIntervalMillis =
      PropertiesHelper.systemHelper.getInt(
          DHTConstants.jvmMonitorMaxIntervalMillisProperty, 30 * 1000);
  // private static final int jvmMonitorMaxIntervalMillis = 15 * 60 * 1000;
  // private static final int jvmFinalizationIntervalMillis = 15 * 60 * 1000;
  private static final int jvmFinalizationIntervalMillis = Integer.MAX_VALUE;
  private static final double jvmMonitorLowMemoryThresholdMB = 50;

  private static Logger log = LoggerFactory.getLogger(MemoryManager.class);

  public MemoryManager() {
    monitor =
        new JVMMonitor(
            jvmMonitorMinIntervalMillis,
            jvmMonitorMaxIntervalMillis,
            jvmFinalizationIntervalMillis,
            true,
            jvmMonitorLowMemoryThresholdMB,
            JVMUtil.getGlobalFinalization());
    monitor.addMemoryObserver(this);
  }

  @Override
  public void jvmMemoryLow(boolean isLow) {
    log.warn("Memory low!");
  }

  @Override
  public void jvmMemoryStatus(long bytesFree) {}

  public JVMMonitor getJVMMonitor() {
    return monitor;
  }
}
