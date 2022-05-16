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

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

public class SystemExitGetOsInfo {

  private final SystemExitLogger logger;

  public SystemExitGetOsInfo(SystemExitLogger logger) {
    this.logger = logger;
  }

  private void getStackTrace() {
    logger.info("stack trace:");
    for (StackTraceElement traceElem : Thread.currentThread().getStackTrace()) {
      logger.info("[trace] " + traceElem.toString());
    }
  }

  private void cpuLoad() throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException {
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    ObjectName osObject = ObjectName.getInstance("java.lang:type=OperatingSystem");
    AttributeList attrList = mBeanServer.getAttributes(osObject, new String[] { "SystemCpuLoad" });

    if (!attrList.isEmpty()) {
      Attribute attr = (Attribute) attrList.get(0);
      Double cpuLoad = (Double) attr.getValue();

      if (cpuLoad >= 0) {
        logger.info("[CPU load] " + truncate(cpuLoad * 100) + "%");
        return;
      }
    }

    logger.info("[CPU load] N/A");
  }

  private String truncate(Double value) {
    return String.format(Locale.ROOT, "%.2f", value);
  }

  private void getEnvVars() {
    logger.info("environment variables:");

    for (Map.Entry entry : System.getenv().entrySet()) {
      logger.info("[env] " + entry.getKey() + "=" + entry.getValue());
    }
  }

  public void getInfo() {
    logger.info("System.exit call intercepted");

    getStackTrace();

    try {
      cpuLoad();
    } catch (Exception e) {
      e.printStackTrace();
    }

    getEnvVars();
  }

  private static List<String> DirectAllowedPatterns = new ArrayList<>();
  static {
    // we need to allow intellij's JUnitStarter and CommandLineWrapper.main methods to exit otherwise the process will
    // never end. (n.b. unit tests are never run on this thread)
    DirectAllowedPatterns.add("JUnitStarter.main");
    DirectAllowedPatterns.add("ScalaTestRunner.main");
    // StallDetector is allowed to kill a process if there is 'no progress' after a certain amount of time
    DirectAllowedPatterns.add("optimus.graph.diagnostics.DefaultStallDetector");
    DirectAllowedPatterns.add("optimus.buildtool.testrunner.worker.OptimusTestWorker");
  }
  private final List<String> anywhereAllowedPatterns = loadPatternsFromFile("anywhere-allowed-patterns");
  private List<String> loadPatternsFromFile(String patternName) {
    Properties properties = new Properties();
    try {
      InputStream is = ClassLoader.getSystemResourceAsStream("systemexit.properties");
      properties.load(is);
    } catch (IOException | NullPointerException ex) {
      new ArrayList<String>();
    }
    return Arrays.asList(properties.getProperty(patternName).split(","));
  }

  // we allow System.exit if called *directly* from any of the DirectAllowedPatterns method patterns,
  // and if called anywhere from the AnywhereAllowedPatterns (which are loaded from file, if it exists,  in case they
  // are business-related classes)
  // TODO (OPTIMUS-30207): use StackWalker instead of this
  private boolean isAllowedExit() {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    int systemExitPosition = Integer.MAX_VALUE;
    for (int i = 0; i < stackTrace.length; i++) {
      String elem = stackTrace[i].toString();
      for (String allowlistedPattern : anywhereAllowedPatterns) {
        if (elem.contains(allowlistedPattern))
          return true;
      }

      if (elem.contains("optimus.systemexit.SystemExitReplacement.exit")) {
        systemExitPosition = i;
      } else if (i == systemExitPosition + 1) {
        for (String allowlistedPattern : DirectAllowedPatterns) {
          if (elem.contains(allowlistedPattern))
            return true;
        }
      }
    }
    return false;
  }

  public SystemExitStrategy getSystemExitStrategy() {
    String exitInterceptProp = System.getProperty(ExitInterceptProp.name);
    if (exitInterceptProp != null) {
      exitInterceptProp = exitInterceptProp.toLowerCase();
    }
    boolean isAllowedExit = isAllowedExit();

    if (!isAllowedExit && ExitInterceptProp.interceptAll.equals(exitInterceptProp)) {
      return SystemExitStrategy.LOG_AND_THROW;
    }
    return SystemExitStrategy.EXIT;
  }

}
