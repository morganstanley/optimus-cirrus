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
package com.ms.silverking.cloud.zookeeper;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import com.ms.silverking.io.FileUtil;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.ParseExceptionAction;
import org.apache.zookeeper.server.LocalZookeeperServerMain;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.admin.AdminServer;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalZKImpl {
  private static final int presumedSuccessTimeMillis = 2 * 1000;

  private static Logger log = LoggerFactory.getLogger(LocalZKImpl.class);

  private static int minStartPort = 10000;
  private static int maxStartPort = 12000;
  private static int maxAttempts = 1000;

  public static final String zkPortProperty = LocalZKImpl.class.getName() + ".ZKPort";
  public static final int defaultZKPort = 0;
  private static final int zkPort;

  private static LocalZookeeperServerMain mainZkServer;

  static {
    zkPort =
        PropertiesHelper.systemHelper.getInt(
            zkPortProperty, defaultZKPort, ParseExceptionAction.RethrowParseException);
    System.setProperty("zookeeper.admin.enableServer", "false");
  }

  public static int startLocalZK(String dataDirBase) {
    int port;
    boolean success;

    success = false;
    if (zkPort == 0) {
      port = ThreadLocalRandom.current().nextInt(minStartPort, maxStartPort);
      for (int i = 0; i < maxAttempts; i++) {
        if (startLocalZK(port, dataDirBase + "/" + port)) {
          success = true;
          break;
        }
      }
    } else {
      port = zkPort;
      if (startLocalZK(port, dataDirBase + "/" + port)) {
        success = true;
      }
    }
    if (success) {
      return port;
    } else {
      throw new RuntimeException("LocalZKImpl.startLocalZK() failed");
    }
  }

  public static boolean startLocalZK(int port, String dataDir) {
    return new LocalZKImpl()._startLocalZK(port, dataDir);
  }

  private LocalZKImpl() {}

  private boolean _startLocalZK(int port, String dataDirName) {
    Starter starter;
    File dataDir;

    dataDir = new File(dataDirName);
    dataDir.mkdirs();
    FileUtil.cleanDirectory(dataDir);
    starter = new Starter(port, dataDirName);
    return starter.waitForStartup();
  }

  private class Starter implements Runnable {
    private final int port;
    private final String dataDir;
    private boolean failed;

    Starter(int port, String dataDir) {
      this.port = port;
      this.dataDir = dataDir;
      new Thread(this, "LocalZKImpl.Starter").start();
    }

    private void startLocalZK(int port, String dataDir) {
      String[] args;

      args = new String[2];
      args[0] = Integer.toString(port);
      args[1] = dataDir;
      log.info("startLocalZK {} {}", port, dataDir);
      mainZkServer = new LocalZookeeperServerMain();
      ServerConfig config = new ServerConfig();
      config.parse(args);
      try {
        mainZkServer.runFromConfig(config);
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (AdminServer.AdminServerException a) {
        throw new RuntimeException(a);
      }
    }

    public void run() {
      startLocalZK(port, dataDir);
      failed = true;
    }

    public boolean waitForStartup() {
      ThreadUtil.sleep(presumedSuccessTimeMillis);
      return !failed;
    }
  }

  public static void shutdown() {
    mainZkServer.shutdownZk();
    mainZkServer = null;
  }

  public static void main(String[] args) {
    CmdLineParser parser;
    LocalZKOptions options;

    options = new LocalZKOptions();
    parser = new CmdLineParser(options);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException cle) {
      System.err.println(cle.getMessage());
      parser.printUsage(System.err);
      System.exit(-1);
    }

    if (options.dataDir == null) {
      parser.printUsage(System.err);
      System.exit(-1);
    }

    System.out.printf("ZK started %s\n", startLocalZK(options.port, options.dataDir));
    /*
        org.apache.zookeeper.server.quorum.QuorumPeerMain
        if [ "x$2" != "x" ]
                then
                    ZOOCFG=$ZOOCFGDIR/$2
                fi
                echo "Using config: $ZOOCFG"

        start)
        echo  "Starting zookeeper ... "
        java  "-Dzookeeper.log.dir=${ZOO_LOG_DIR}" "-Dzookeeper.root.logger=${ZOO_LOG4J_PROP}" \
        -cp $CLASSPATH $JVMFLAGS $ZOOMAIN $ZOOCFG >$ZOOLOG &
        echo $! > $ZOOPIDFILE
        echo STARTED
        ;;
    */
  }
}
