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
package com.ms.silverking.cloud.dht.meta;

import com.ms.silverking.cloud.meta.ValueListener;
import com.ms.silverking.cloud.meta.ValueWatcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DHTRingCurTargetWatcher implements ValueListener {
  private final String dhtName;
  private final DHTConfiguration dhtConfig;
  private final MetaClient mc;
  private final ValueWatcher curRingWatcher;
  private ValueWatcher targetRingWatcher;
  private final DHTRingCurTargetListener listener;

  private static Logger log = LoggerFactory.getLogger(DHTRingCurTargetWatcher.class);

  private static final int initialIntervalMillis = 10 * 1000;
  private static final int checkIntervalMillis = 1 * 60 * 1000;
  private static final boolean debug = true;

  public DHTRingCurTargetWatcher(
      MetaClient mc,
      String dhtName,
      DHTConfiguration dhtConfig,
      DHTRingCurTargetListener listener) {
    this.dhtName = dhtName;
    this.dhtConfig = dhtConfig;
    this.mc = mc;
    curRingWatcher =
        new ValueWatcher(
            mc,
            MetaPaths.getInstanceCurRingAndVersionPairPath(dhtName),
            this,
            checkIntervalMillis,
            initialIntervalMillis);
    this.listener = listener;
  }

  public void startTargetRingWatcher() {
    targetRingWatcher =
        new ValueWatcher(
            mc,
            MetaPaths.getInstanceTargetRingAndVersionPairPath(dhtName),
            this,
            checkIntervalMillis,
            initialIntervalMillis);
  }

  @Override
  public void newValue(String basePath, byte[] value, Stat stat) {
    if (debug) {
      log.info("DHTRingCurTargetWatcher.newValue: {}", basePath);
    }
    if (value.length < DHTRingCurTargetZK.minValueLength) {
      log.info("Value length too small. {} {}", basePath, value.length);
    } else {
      if (basePath.equals(MetaPaths.getInstanceCurRingAndVersionPairPath(dhtName))) {
        if (debug) {
          log.info("DHTRingCurTargetWatcher.newCurRingAndVersion: {}", basePath);
        }
        listener.newCurRingAndVersion(DHTRingCurTargetZK.bytesToNameAndVersion(value));
      } else if (basePath.equals(MetaPaths.getInstanceTargetRingAndVersionPairPath(dhtName))) {
        if (debug) {
          log.info("DHTRingCurTargetWatcher.newTargetRingAndVersion: {}", basePath);
        }
        listener.newTargetRingAndVersion(DHTRingCurTargetZK.bytesToNameAndVersion(value));
      } else {
        log.info("Unexpected value update in DHTRingCurTargetWatcher: {}", basePath);
        log.info("{}", MetaPaths.getInstanceCurRingAndVersionPairPath(dhtName));
        log.info("{}", MetaPaths.getInstanceTargetRingAndVersionPairPath(dhtName));
      }
    }
  }
}
