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
package optimus.systemexit;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Standard logger for System.exit instrumentation. Logs to the standard error output
 * based on the level specified by exit.intercept.log.level system property (DEBUG, INFO,
 * WARN or ERROR). If exit.intercept.log.file system property
 */
public class StandardSystemExitLogger implements SystemExitLogger {

  enum LogLevel {
    DEBUG(0),
    INFO(1),
    WARN(2),
    ERROR(3);

    private int severity;

    LogLevel(int severity) {
      this.severity = severity;
    }

    public boolean aboveOr(LogLevel that) {
      return this.severity >= that.severity;
    }
  }

  private String logFileProperty = "exit.intercept.log.file";
  private String logLevelProperty = "exit.intercept.log.level";

  private LogLevel logLevel = LogLevel.INFO;
  private File logFile = null;

  public StandardSystemExitLogger() {
    String logFilePath = System.getProperty(logFileProperty);

    if (logFilePath != null) {
      File file = new File(logFilePath);
      if (isValidLogFile(file)) {
        logFile = file;
      } else {
        warn("Log file for System.exit intercepts does not exist (" + logFileProperty + " property) for the path: " +
            logFilePath);
      }
    }

    String logLevelValue = System.getProperty(logLevelProperty);

    if (logLevelValue != null) {
      try {
        logLevel = LogLevel.valueOf(logLevelValue.toUpperCase());
      } catch (IllegalArgumentException e) {
        warn("Invalid value specified for " + logLevelProperty + " property");
      }
    }
  }

  @Override
  public void debug(String msg) {
    if (LogLevel.DEBUG.aboveOr(logLevel))
      logOutput("[DEBUG] " + msg);
  }

  @Override
  public void info(String msg) {
    if (LogLevel.INFO.aboveOr(logLevel))
      logOutput("[INFO] " + msg);
  }

  @Override
  public void warn(String msg) {
    if (LogLevel.WARN.aboveOr(logLevel))
      logOutput("[WARN] " + msg);
  }

  private void logOutput(String msg) {
    System.err.println(msg);
    logToFile(msg);
  }

  private boolean isValidLogFile(File file) {
    return file.exists() && file.isFile() && file.canWrite();
  }

  private void logToFile(String msg) {
    if (logFile != null && isValidLogFile(logFile)) {
      PrintWriter pw = null;

      try {
        FileWriter fw = new FileWriter(logFile.getAbsolutePath(), true);
        pw = new PrintWriter(new BufferedWriter(fw));
        pw.println(msg);
      } catch (IOException e) {
        warn("Could not write to System.exit intercepts log file (" + logFileProperty + " property)");
      } finally {
        if (pw != null) pw.close();
      }
    }
  }

}
