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

import java.io.IOException;

import com.ms.silverking.collection.Pair;
import com.ms.silverking.process.ProcessExecutor;
import org.apache.zookeeper.Shell.ExitCodeException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class Ping {
  private static final String ping = "/bin/ping";
  private static final int defaultCount = 10;

  private static Logger log = LoggerFactory.getLogger(Ping.class);

  public void ping(String dest) throws IOException {
    ping(dest, defaultCount);
  }

  public Pair<Boolean, String> ping(String dest, int count) throws IOException {
    ProcessExecutor pe;
    String[] cmd;
    boolean ok;
    String output;

    cmd = new String[4];
    cmd[0] = ping;
    cmd[1] = "-c";
    cmd[2] = Integer.toString(count);
    cmd[3] = dest;
    pe = new ProcessExecutor(cmd);
    try {
      pe.execute();
      ok = pe.getExitCode() == 0;
      output = pe.getOutput();
    } catch (ExitCodeException ece) {
      ok = false;
      output = null;
    }
    return new Pair<>(ok, output);
  }

  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("args: <target> <count>");
    } else {
      try {
        Ping ping;
        String dest;
        int count;

        dest = args[0];
        count = Integer.parseInt(args[1]);
        ping = new Ping();
        System.out.println(ping.ping(dest, count));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
