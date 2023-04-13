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
package com.ms.silverking.pssh;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.thread.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwoLevelParallelSSHWorker implements Runnable {
  private ParallelSSHBase sshBase;

  private static Logger log = LoggerFactory.getLogger(TwoLevelParallelSSHWorker.class);

  private static final int resultErrorCode = 127;

  private boolean running;
  private int timeoutSeconds;
  private AtomicInteger active;
  private AtomicInteger runningThreads;
  private String myHost;

  private SSHManager sshManager;

  static {
    String base;
    String myStderr;
    String myStdout;

    base = "/tmp/worker." + System.currentTimeMillis();
    myStdout = base + ".stdout";
    myStderr = base + ".stderr";
    try {
      System.setOut(new PrintStream(new FileOutputStream(myStdout)));
      System.setErr(new PrintStream(new FileOutputStream(myStderr)));
    } catch (FileNotFoundException fnfe) {
      fnfe.printStackTrace();
    }
  }

  public TwoLevelParallelSSHWorker(String managerURL, int numWorkerThreads, int timeoutSeconds) throws Exception {
    Runtime.getRuntime().addShutdownHook(new ShutdownHook());

    this.timeoutSeconds = timeoutSeconds;

    active = new AtomicInteger();
    runningThreads = new AtomicInteger();

    myHost = InetAddress.getLocalHost().getCanonicalHostName();

    sshManager = findManager(managerURL);
    sshBase = new ParallelSSHBase(null, sshManager.getSSHCmdMap(), sshManager.getHostGroups());

    if (sshManager == null) {
      throw new RuntimeException("Manager not found: " + managerURL);
    }

    runningThreads.set(numWorkerThreads);
    running = true;
    for (int i = 0; i < numWorkerThreads; i++) {
      new Thread(this, "ParallelSSH Worker " + i).start();
    }
  }

  private SSHManager findManager(String managerURL) throws Exception {
    return (SSHManager) Naming.lookup(managerURL);
  }

  private HostAndCommand getHostAndCommand() throws RemoteException {
    return sshManager.getHostAndCommand();
  }

  public void run() {
    try {
      while (running) {
        try {
          HostAndCommand hostAndCommand;
          HostResult result;

          log.info("Calling getHost");
          hostAndCommand = getHostAndCommand();
          log.info("back from getHost");
          if (hostAndCommand == null) {
            break;
          } else {
            active.incrementAndGet();
            try {
              int resultCode;

              log.info("  Host: {}  Active: {}", hostAndCommand, active);
              resultCode = sshBase.doSSH(hostAndCommand, timeoutSeconds, true);
              result = resultCode == resultErrorCode
                       ? result = HostResult.failure
                       : HostResult.success;
            } finally {
              active.decrementAndGet();
            }
            sshManager.setHostResult(hostAndCommand, result);
          }
        } catch (RemoteException re) {
          log.error("", re);
          break;
        }
      }
    } finally {
      int running;

      log.info("WorkerThread complete {}", myHost);
      System.out.flush();
      log.info("WorkerThread complete");
      running = runningThreads.decrementAndGet();
      if (running == 0) {
        try {
          sshManager.workerComplete();
          log.info("Worker complete {}  {}", myHost, new Date());
          System.out.flush();
          log.info("Worker complete");
        } catch (RemoteException re) {
          log.error("", re);
          ThreadUtil.pauseAfterException();
        }
      }
    }
  }

  public class ShutdownHook extends Thread {
    public void ShutdownHook() {
    }

    public void run() {
      log.info("Shutdown");
    }
  }

  // ////////////////////////////////////////////////////////////////////

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      String managerURL;
      int numWorkerThreads;
      int timeoutSeconds;
      TwoLevelParallelSSHWorker parallelSSH;

      if (args.length != 3) {
        System.out.println("<managerURL> <numWorkerThreads> <timeoutSeconds>");
        return;
      }
      managerURL = args[0];
      numWorkerThreads = Integer.parseInt(args[1]);
      timeoutSeconds = Integer.parseInt(args[2]);
      parallelSSH = new TwoLevelParallelSSHWorker(managerURL, numWorkerThreads, timeoutSeconds);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
