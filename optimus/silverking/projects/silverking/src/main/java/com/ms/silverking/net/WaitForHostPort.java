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
package com.ms.silverking.net;

import java.net.Socket;
import java.util.concurrent.TimeUnit;

import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.time.SimpleTimer;
import com.ms.silverking.time.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WaitForHostPort {
  private static final int successExitCode = 0;
  private static final int errorExitCode = -1;
  private static final int pollIntervalMillis = 1 * 1000;

  private static Logger log = LoggerFactory.getLogger(WaitForHostPort.class);

  private static boolean canConnect(HostAndPort hostAndPort) {
    try {
      Socket s;

      s = new Socket(hostAndPort.getHostName(), hostAndPort.getPort());
      s.close();
      return true;
    } catch (Exception e) {
      log.debug("",e);
      return false;
    }
  }

  public static int doWait(HostAndPort hostAndPort, int timeoutSeconds) {
    Timer sw;

    sw = new SimpleTimer(TimeUnit.SECONDS, timeoutSeconds);
    while (!sw.hasExpired()) {
      if (canConnect(hostAndPort)) {
        return successExitCode;
      }
      ThreadUtil.sleep(pollIntervalMillis);
    }
    return errorExitCode;
  }

  public static void main(String[] args) {
    int exitCode;

    exitCode = errorExitCode;
    if (args.length != 2) {
      log.info("args: <hostAndPort> <timeoutSeconds>");
    } else {
      HostAndPort hostAndPort;
      int timeoutSeconds;

      hostAndPort = new HostAndPort(args[0]);
      timeoutSeconds = Integer.parseInt(args[1]);
      try {
        exitCode = doWait(hostAndPort, timeoutSeconds);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    System.exit(exitCode);
  }
}
