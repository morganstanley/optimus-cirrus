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
package com.ms.silverking.cloud.meta;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfiguration;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfigurationUtil;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.thread.ThreadUtil;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExclusionFileMonitor {
  private ExclusionZK exclusionZK;
  private final int watchIntervalSeconds;
  private final File exclusionFile;

  private static Logger log = LoggerFactory.getLogger(ExclusionFileMonitor.class);

  private static final String logFileName = "ExclusionFileMonitor.out";

  public ExclusionFileMonitor(
      SKGridConfiguration gc, int watchIntervalSeconds, String exclusionFile)
      throws IOException, KeeperException {
    MetaClient mc;
    NamedRingConfiguration ringConfig;

    this.watchIntervalSeconds = watchIntervalSeconds;
    this.exclusionFile = new File(exclusionFile);
    log.info("exclusionFile {}", exclusionFile);

    ringConfig = NamedRingConfigurationUtil.fromGridConfiguration(gc);

    mc =
        new MetaClient(
            ringConfig.getRingConfiguration().getCloudConfiguration(),
            gc.getClientDHTConfiguration().getZKConfig());

    exclusionZK = new ExclusionZK(mc);
  }

  public void monitor() {
    while (true) {
      try {
        updateCloudExclusionSet();
        ThreadUtil.sleepSeconds(watchIntervalSeconds);
      } catch (Exception e) {
        log.error("", e);
      }
    }
  }

  private void updateCloudExclusionSet() {
    try {
      ExclusionSet exclusionSet;
      ExclusionSet newExclusionSet;
      Set<String> excludedInFiles;

      excludedInFiles = getCurrentlyExcludedInFiles();
      log.info("Excluded in files:");
      log.info("{}", CollectionUtil.toString(excludedInFiles));
      exclusionSet = exclusionZK.readLatestFromZK();
      newExclusionSet = new ExclusionSet(excludedInFiles, -1, exclusionSet.getMzxid());
      if (!newExclusionSet.equals(exclusionSet)) {
        exclusionZK.writeToZK(newExclusionSet);
      }
    } catch (Exception e) {
      log.error("Exception in updateCloudExclusionSet", e);
    }
  }

  private Set<String> getCurrentlyExcludedInFiles() throws IOException {
    return exclusionZK.readFromFile(exclusionFile, 0).getServers();
  }

  //////////////////////

  public static void main(String[] args) {
    try {
      CmdLineParser parser;
      ExclusionFileMonitorOptions options;
      ExclusionFileMonitor exclusionFileMonitor;
      SKGridConfiguration gc;

      options = new ExclusionFileMonitorOptions();
      parser = new CmdLineParser(options);
      try {
        parser.parseArgument(args);
        gc = SKGridConfiguration.parseFile(options.gridConfig);
        exclusionFileMonitor =
            new ExclusionFileMonitor(gc, options.watchIntervalSeconds, options.exclusionFile);
        exclusionFileMonitor.monitor();
      } catch (CmdLineException cle) {
        System.err.println(cle.getMessage());
        parser.printUsage(System.err);
        System.exit(-1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
