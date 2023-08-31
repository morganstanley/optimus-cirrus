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
package com.ms.silverking.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.text.StringUtil;
import com.ms.silverking.time.TimeUtils;
import org.apache.zookeeper.Shell.ShellCommandExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessExecutor {

  private String[] commands;
  private ShellCommandExecutor shellCommandExecutor;

  private static Logger log = LoggerFactory.getLogger(ProcessExecutor.class);

  private static final long noTimeout = 0L;

  public ProcessExecutor(String[] commands) {
    this(commands, noTimeout);
  }

  public ProcessExecutor(String[] commands, long timeoutInSeconds) {
    //        System.out.println("commands: " + Arrays.toString(commands));
    this.commands = commands;
    shellCommandExecutor =
        new ShellCommandExecutor(
            commands, null, null, TimeUtils.secondsInMillis((int) timeoutInSeconds));
  }

  public void execute() throws IOException {
    shellCommandExecutor.execute();
  }

  public String getOutput() {
    return shellCommandExecutor.getOutput();
  }

  public String getTrimmedOutput() {
    return shellCommandExecutor.getOutput().trim();
  }

  public int getExitCode() {
    return shellCommandExecutor.getExitCode();
  }

  public boolean timedOut() {
    return shellCommandExecutor.isTimedOut();
  }

  public String[] getCommands() {
    return commands;
  }

  public static ProcessExecutor bashExecutor(String commands) {
    return bashExecutor(commands, noTimeout);
  }

  public static ProcessExecutor bashExecutor(String commands, long timeoutInSeconds) {
    return new ProcessExecutor(getBashCmd(commands), timeoutInSeconds);
  }

  public static ProcessExecutor bashExecutor(String[] commands, long timeoutInSeconds) {
    return bashExecutor(StringUtil.arrayToString(commands, ' '), timeoutInSeconds);
  }

  public static ProcessExecutor sshExecutor(String server, String commands) {
    return new ProcessExecutor(getSshCommandWithRedirectOutputFile(server, commands));
  }

  /////////////////////////

  public static final String separator = File.separator;
  public static final String newline = System.lineSeparator();

  public static void printDirContents(String header, String dirPath) {
    log.info("  === {}", header);
    log.info("{}", runCmd(new String[] {"/bin/sh", "-c", "ls -lR " + dirPath}));
  }

  // runs either cksum or md5sum on a directory
  public static String runDirSumCmd(String cmd, File dir) {
    String absPath = dir.getAbsolutePath() + separator;
    String out =
        runCmd(
            new String[] {
              "/bin/sh",
              "-c",
              "find "
                  + absPath
                  + " -type f -exec "
                  + cmd
                  + " {} \\; | sed s#"
                  + absPath
                  + "## | sort | "
                  + cmd
            });
    //        System.out.println(out);
    return out.trim();
  }

  ////////////
  // useful for chained commands: |, >, etc.
  ////////////
  public static String runBashCmd(String commands) {
    return runCmd(getBashCmd(commands));
  }

  public static void runBashCmdNoWait(String commands) {
    runCmdNoWait(getBashCmd(commands));
  }

  private static String[] getBashCmd(String commands) {
    //        return runCmd(new String[]{"/bin/bash", "-c", "'" + commands + "'"});    // single
    // quotes around
    //        'commands' messes it up
    return new String[] {"/bin/bash", "-c", commands};
  }

  public static String runSshCmdWithRedirectOutputFile(String server, String commands) {
    return runCmd(getSshCommandWithRedirectOutputFile(server, commands));
  }

  public static String ssh(String server, String commands) {
    return runCmd(getSshCommand(server, commands));
  }

  public static void scpFile(String file, String host, String destDir) {
    String user = System.getProperty("user.name");
    runCmd("scp -o StrictHostKeyChecking=no " + file + " " + user + "@" + host + ":" + destDir);
  }

  public static String runCmd(String cmd, File f) {
    String[] commands = {cmd, f.getAbsolutePath()};
    return runCmd(commands);
  }

  ////////////
  // useful for running a single script/command
  //   that has no spaces, e.g. "~/SilverKing/build/aws/zk_start.sh"
  //   and also something like "date +%H:%M:%S", that has a space
  ////////////
  public static String runCmd(String command) {
    return runCmd(command.split(" "));
  }

  public static String runCmd(String[] commands) {
    return runCmd(commands, true, true);
  }

  public static void runCmdNoWait(String command) {
    runCmdNoWait(new String[] {command});
  }

  public static void runCmdNoWait(String[] commands) {
    runCmd(commands, false, false);
  }

  // https://stackoverflow.com/questions/5928225/how-to-make-pipes-work-with-runtime-exec
  //  - single commands can be run with:
  //    "String"   - i.e. runTime.exec("ssh-keygen -y -f /home/ec2-user/.ssh/id_rsa")
  //  - chained commands, like pipe or cat or append, etc.. you have to use:
  //    "String[]" - i.e. runTime.exec("ssh-keygen -y -f /home/ec2-user/.ssh/id_rsa >>
  // /home/ec2-user/
  //    .ssh/authorized_keys") fails. you have to do
  //                      runTime.exec(new String[]{"/bin/sh", "-c", "ssh-keygen -y -f
  // /home/ec2-user/.ssh/id_rsa >>
  //                      /home/ec2-user/.ssh/authorized_keys"})
  //
  // http://stackoverflow.com/questions/5711084/java-runtime-getruntime-getting-output-from-executing-a-command-line
  // -program
  private static String runCmd(String[] commands, boolean captureOutput, boolean wait) {
    //        System.out.println("commands: " + Arrays.toString(commands));
    StringBuffer output = new StringBuffer();
    Runtime runTime = Runtime.getRuntime();
    try {
      Process p;
      if (commands.length == 1) p = runTime.exec(commands[0]);
      else p = runTime.exec(commands);

      if (wait) p.waitFor();

      if (captureOutput) {
        InputStream inputStream = p.getInputStream();
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(inputStream));
        String line = null;
        while ((line = stdInput.readLine()) != null) output.append(line + newline);
        inputStream.close();
      }

      if (p.exitValue() != 0) {
        log.info("Commands: {}  exited with code = {}", Arrays.toString(commands), p.exitValue());
        InputStream errorStream = p.getErrorStream();
        BufferedReader stdErr = new BufferedReader(new InputStreamReader(errorStream));
        String line = null;
        while ((line = stdErr.readLine()) != null) log.info("{}", line);
        errorStream.close();
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    } catch (IllegalThreadStateException e) {
      // if we don't wait and process is still running, we'll hit this
    }

    return output.toString();
  }

  public static String[] getSshCommandWithRedirectOutputFile(String server, String commands) {
    //        return new String[]{"ssh -v -x -o StrictHostKeyChecking=no " + server + " \"/bin/bash
    // -c '" + commands
    //        + "'\""};    // quotes are important around commands
    //        return new String[]{"ssh", "-v", "-x", "-o", "StrictHostKeyChecking=no", server,
    // "/bin/bash", "-c", "'"
    //        + commands + "'"};    // quotes are important around commands
    return new String[] {
      "ssh",
      "-v",
      "-x",
      "-o",
      "StrictHostKeyChecking=no",
      server,
      "/bin/bash -c '" + commands + "'",
      " > /tmp/ssh.out." + SystemTimeUtil.skSystemTimeSource.absTimeNanos()
    }; // quotes are important around
    // commands
  }

  public static String[] getSshCommand(String server, String commands) {
    return new String[] {
      "ssh", "-x", "-o", "StrictHostKeyChecking=no", server, "/bin/bash", "-c", "'" + commands + "'"
    }; // quotes are important around commands
  }
}
