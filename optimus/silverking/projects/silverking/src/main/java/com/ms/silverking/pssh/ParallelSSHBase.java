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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import com.google.common.collect.ImmutableMap;
import com.ms.silverking.cloud.config.HostGroupTable;
import com.ms.silverking.collection.MapUtil;
import com.ms.silverking.io.IORelay;
import com.ms.silverking.os.linux.redhat.RedHat;
import com.ms.silverking.process.ProcessWaiter;
import com.ms.silverking.text.StringUtil;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.util.PropertiesHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelSSHBase {
  private String sshCmd;
  private Map<String, String> sshCmdMap;
  private HostGroupTable hostGroups;
  private Set<String> successful;
  private Set<String> failed;
  private Set<String> completed;

  private static Logger log = LoggerFactory.getLogger(ParallelSSHBase.class);

  public static final int errorCode = 127;
  public static final int exceptionErrorCode = 127;

  public static String timeoutVar = "TIMEOUT";
  public static String hostnameVar = "HOSTNAME";

  private static final String sshEnvVar = "SK_PSSH_SSH";
  private static final String defaultSSHCmdRH5 = "ssh -x -o StrictHostKeyChecking=no " + hostnameVar;
  private static final String defaultSSHCmd =
      "timeout " + timeoutVar + " ssh -x -o StrictHostKeyChecking=no " + hostnameVar;
  private static final String globalSSHCmd;
  private static final String sshMapEnvVar = "SK_PSSH_SSH_MAP";
  private static final Map<String, String> globalSSHCmdMap;
  private static final char mapDelimiter = '\t';

  private static final boolean globalSSHCmdIsDefault;
  private final boolean sshCmdIsDefault;

  static {
    String mapFile;

    if (RedHat.getRedHatVersion() >= 6.0) {
      globalSSHCmd = PropertiesHelper.envHelper.getString(sshEnvVar, defaultSSHCmd);
      globalSSHCmdIsDefault = globalSSHCmd.equals(defaultSSHCmd);
    } else {
      globalSSHCmd = PropertiesHelper.envHelper.getString(sshEnvVar, defaultSSHCmdRH5);
      globalSSHCmdIsDefault = globalSSHCmd.equals(defaultSSHCmdRH5);
    }
    mapFile = PropertiesHelper.envHelper.getString(sshMapEnvVar, PropertiesHelper.UndefinedAction.ZeroOnUndefined);
    if (mapFile != null && mapFile.trim().length() != 0) {
      log.info("mapFile {}", mapFile);
      try {
        globalSSHCmdMap = MapUtil.parseStringMap(new FileInputStream(mapFile), mapDelimiter,
            MapUtil.NoDelimiterAction.Ignore);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    } else {
      log.info("mapFile null or empty");
      globalSSHCmdMap = ImmutableMap.of();
    }
  }

  public ParallelSSHBase(String sshCmd, Map<String, String> sshCmdMap, HostGroupTable hostGroups) {
    this.sshCmd = sshCmd == null ? globalSSHCmd : sshCmd;
    if (sshCmd == null) {
      this.sshCmd = globalSSHCmd;
      sshCmdIsDefault = globalSSHCmdIsDefault;
    } else {
      this.sshCmd = sshCmd;
      sshCmdIsDefault = false;
    }
    this.sshCmdMap = sshCmdMap;
    this.hostGroups = hostGroups;
    successful = new ConcurrentSkipListSet<String>();
    failed = new ConcurrentSkipListSet<String>();
    completed = new ConcurrentSkipListSet<String>();
  }

  public ParallelSSHBase(HostGroupTable hostGroups) {
    this(globalSSHCmd, globalSSHCmdMap, hostGroups);
  }

  public String getSSHCmd() {
    return sshCmd;
  }

  public boolean sshCmdIsDefault() {
    return sshCmdIsDefault;
  }

  public Map<String, String> getSSHCmdMap() {
    return sshCmdMap;
  }

  public HostGroupTable getHostGroups() {
    return hostGroups;
  }

  private String arrayToString(String[] cmd, char quoteChar) {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append(quoteChar);
    for (String c : cmd) {
      sb.append(c);
      sb.append(' ');
    }
    sb.setLength(sb.length() - 1);
    sb.append(quoteChar);
    return sb.toString();
  }

  private String[] sshCmd(String hostname, String[] cmd, int timeoutSeconds) {
    return sshCmd(hostname, cmd, timeoutSeconds, false);
  }

  private String[] sshCmd(String hostname, String[] cmd, int timeoutSeconds, boolean quotedBash) {
    List<String> _sshCmd;
    String resolvedSSHCmd;
    String hostSSHCmd;

    hostSSHCmd = sshCmdMap.get(hostname);
    if (hostSSHCmd == null) {
      if (hostGroups != null) {
        log.info("sshhg non-null");
        for (String hostGroup : hostGroups.getHostGroups(hostname)) {
          String hostGroupSSHCmd;

          log.info("sshhg hg {}", hostGroup);
          hostGroupSSHCmd = sshCmdMap.get(hostGroup);
          if (hostGroupSSHCmd != null) {
            log.info("sshhg hg cmd {}", hostGroupSSHCmd);
            hostSSHCmd = hostGroupSSHCmd;
            break;
          }
        }
        if (hostSSHCmd == null) {
          hostSSHCmd = sshCmd;
        }
      } else {
        log.info("sshhg null");
        hostSSHCmd = sshCmd;
      }
    }
    resolvedSSHCmd = hostSSHCmd.replaceAll(timeoutVar, Integer.toString(timeoutSeconds)).replaceAll(hostnameVar,
        hostname);
    _sshCmd = new ArrayList<>();
    for (String s : resolvedSSHCmd.split("\\s+")) {
      _sshCmd.add(s);
    }
    if (!quotedBash) {
      _sshCmd.add("/bin/bash");
      _sshCmd.add("-c");
      _sshCmd.add(arrayToString(cmd, '\''));
      _sshCmd.add("< /dev/null");
    } else {
      _sshCmd.add("/bin/bash -c " + arrayToString(cmd, '\''));
      _sshCmd.add("< /dev/null");
    }

    return _sshCmd.toArray(new String[0]);
  }

  public int doSSH(HostAndCommand hostAndCommand, int timeoutSeconds, boolean quotedBash) {
    return doSSH(hostAndCommand.getHost(), hostAndCommand.getCommand(), timeoutSeconds, quotedBash);
  }

  public int doSSH(String host, String[] cmd, int timeoutSeconds) {
    return doSSH(host, cmd, timeoutSeconds, false);
  }

  public int doSSH(String host, String[] cmd, int timeoutSeconds, boolean quotedBash) {
    boolean success;

    success = false;
    try {
      Process execProc;
      int result;
      String[] ssh;
      ProcessBuilder pb;
      ProcessWaiter waiter;

      log.info(" ****************************************");
      log.info("Host: {}" , host);
      ssh = sshCmd(host, cmd, timeoutSeconds, quotedBash);
      log.info(StringUtil.arrayToQuotedString(ssh));
      pb = new ProcessBuilder(ssh);
      execProc = pb.start();
      waiter = new ProcessWaiter(execProc);
      new IORelay(execProc.getInputStream(), System.out, false);
      new IORelay(execProc.getErrorStream(), System.err, false);
      result = waiter.waitFor((timeoutSeconds + 1) * 1000);
      if (result == ProcessWaiter.TIMEOUT) {
        execProc.destroy();
        success = false;
      } else {
        success = result != errorCode;
      }
      execProc.getInputStream().close();
      execProc.getErrorStream().close();
      log.info("Result: {}  {}" , result , host);
      return result;
    } catch (Exception e) {
      log.error("",e);
      return exceptionErrorCode;
    } finally {
      completed.add(host);
      if (success) {
        successful.add(host);
      } else {
        failed.add(host);
      }
    }
  }

  public int numCompleted() {
    return completed.size();
  }

  public Set<String> getFailed() {
    return failed;
  }

  public void waitForCompletion(int toComplete) {
    int lastCompleted;

    lastCompleted = completed.size();
    while (completed.size() < toComplete) {
      if (completed.size() != lastCompleted) {
        log.info("waitForCompletion: {}/{}" , completed.size() ,toComplete);
        lastCompleted = completed.size();
      }
      ThreadUtil.sleep(1000); // fast notification is not important
    }
  }

  public void doSSH(List<String> hosts, String[] cmd, int timeoutSeconds) {
    for (String host : hosts) {
      doSSH(host, cmd, timeoutSeconds);
    }
  }

  public static void main(String[] args) {
    System.out.println(System.getProperty("os.name"));
    System.out.println(System.getProperty("os.arch"));
  }
}
