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

import java.util.Arrays;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.config.HostGroupTable;
import com.ms.silverking.io.StreamParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelSSH extends ParallelSSHBase implements Runnable {
  private boolean running;
  private Queue<String> hosts;
  private String[] command;
  private int timeoutSeconds;
  private AtomicInteger active;

  private static Logger log = LoggerFactory.getLogger(ParallelSSH.class);

  public ParallelSSH(
      Set<String> hosts,
      String[] command,
      int numWorkerThreads,
      int timeoutSeconds,
      HostGroupTable hostGroups) {
    super(hostGroups);
    this.hosts = new ArrayBlockingQueue<String>(hosts.size(), false, hosts);
    this.command = command;
    this.timeoutSeconds = timeoutSeconds;
    active = new AtomicInteger();
    running = true;
    for (int i = 0; i < numWorkerThreads; i++) {
      new Thread(this, "ParallelSSH Worker " + i).start();
    }
  }

  public void run() {
    while (running) {
      String host;

      host = hosts.poll();
      if (host == null) {
        break;
      } else {
        active.incrementAndGet();
        try {
          log.info("Remaining: {}   Active: {}", hosts.size(), active);
          doSSH(host, command, timeoutSeconds);
        } finally {
          log.info("Complete: {}", host);
          active.decrementAndGet();
        }
      }
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      Set<String> hosts;
      int numWorkerThreads;
      int timeoutSeconds;
      String[] cmd;
      ParallelSSH parallelSSH;

      if (args.length < 4) {
        log.info("<hostsFile> <numWorkerThreads> <timeoutSeconds> <cmd...>");
        return;
      }
      hosts = ImmutableSet.copyOf(StreamParser.parseFileLines(args[0]));
      numWorkerThreads = Integer.parseInt(args[1]);
      timeoutSeconds = Integer.parseInt(args[2]);
      cmd = Arrays.copyOfRange(args, 3, args.length);
      parallelSSH = new ParallelSSH(hosts, cmd, numWorkerThreads, timeoutSeconds, null);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
