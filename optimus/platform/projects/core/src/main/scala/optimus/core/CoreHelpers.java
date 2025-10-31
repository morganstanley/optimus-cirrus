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
package optimus.core;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import optimus.breadcrumbs.Breadcrumbs;
import optimus.breadcrumbs.ChainedID$;
import optimus.breadcrumbs.crumbs.Crumb;
import optimus.breadcrumbs.crumbs.Properties;
import optimus.breadcrumbs.crumbs.Properties.Elems;
import optimus.breadcrumbs.crumbs.PropertiesCrumb;
import optimus.graph.InstancePropertyTarget;
import optimus.graph.PThreadContext;
import optimus.graph.PredicatedPropertyTweakTarget;
import optimus.graph.PropertyTarget;
import optimus.graph.Settings;
import optimus.graph.TwkPropertyInfo0;
import optimus.logging.LoggingInfo;
import optimus.platform.Tweak;
import optimus.platform.util.Version$;
import optimus.scalacompat.collection.javaapi.CollectionConverters;
import optimus.utils.compat.VMSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CoreHelpers {
  public static final MethodHandle intern_mh;

  static {
    try {
      var lookup = MethodHandles.lookup();
      var intern_mt = MethodType.methodType(Object.class, Object.class);
      intern_mh = lookup.findStatic(CoreHelpers.class, "intern", intern_mt);
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private static final ConcurrentHashMap<String, ZoneId> _zoneIDs = new ConcurrentHashMap<>();

  // com.google.common.collect.int
  private static final WeakInterner interner = new WeakInterner();
  private static final Interner<Object> strongInterner = Interners.newStrongInterner();

  private static final Logger log = LoggerFactory.getLogger(Settings.class);

  private static final int MAX_CHARS = 200000;

  /* By default HANG will write out file just once, but sometimes it's convenient to 'watch' breakpoints by just
  watching file directory  */
  private static boolean doSendDebugInfo = true;
  /** e.g. The grid engine launcher sets this to globally visible compile space */
  private static final String OPTIMUS_DBG_DIR = System.getenv("OPTIMUS_DBG_DIR");

  private static final int STOP_MILLISECONDS = 100;

  /**
   * Find/Computes cached value of the zoneId
   *
   * <p>It's not clear why java doesn't do it by default
   */
  public static ZoneId getZoneID(String zoneId) {
    return _zoneIDs.computeIfAbsent(zoneId, ZoneId::of);
  }

  /** Intern any object into a weak table, more logic could be added here... */
  public static Object intern(Object src) {
    return src == null ? null : interner.intern(src);
  }

  /** Intern any object into a weak table, not List will match Seq etc... */
  public static Object nonStrictIntern(Object src) {
    return src == null ? null : interner.nonStrictIntern(src);
  }

  /** Intern any object into a strong table, more logic could be added here... */
  public static Object strongIntern(Object src) {
    return src == null ? null : strongInterner.intern(src);
  }

  public static <T> boolean arraysAreEqual(T[] a, T[] b) {
    return Arrays.equals(a, b);
  }

  public static String safeToString(Object o) {
    return safeToString(o, null, MAX_CHARS);
  }

  public static String safeToString(Object o, String whenFailed, int maxChars) {
    if (o == null) return "[null]";
    try {
      String fullString;
      if (o.getClass().isArray()) {
        Object[] a = (Object[]) o;
        fullString = o.getClass().getComponentType().getName() + "[" + a.length + "]";
      } else fullString = o.toString();

      if (fullString.length() > maxChars) return fullString.substring(0, maxChars) + "...";
      else return fullString;
    } catch (Throwable ex) {
      return (whenFailed == null)
          ? ex.toString()
          : o.getClass().getName() + whenFailed + " Exception was: " + ex;
    }
  }

  public static void registerGCListener(NotificationListener gcListener) {
    List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean gcBean : gcBeans) {
      ((NotificationEmitter) gcBean).addNotificationListener(gcListener, null, null);
    }
  }

  /** This can be set in a debugger to cause next iteration to stop or not */
  private static boolean breakOn = true;

  private static String port = null;
  private static final long breakMaxMs =
      Long.parseLong(
              Optional.ofNullable(System.getenv("OPTIMUS_COREHELPERS_BREAK_MAX_MINUTES"))
                  .orElse("30"))
          * 60L
          * 1000L;
  private static long breakTotalMs = 0L;
  private static final Crumb.Source source = Crumb.newSource("debuggerBreak");

  @SuppressWarnings("unused")
  public static void debuggerBreak(String info) {
    if (!breakOn) return;
    try {
      if (doSendDebugInfo) {
        sendDebugInfo(info);
        doSendDebugInfo = false;
      }
      if (port != null) {
        log.warn("Entering debuggerBreak: {}", info);
        do {
          // When attached you would actually set breakpoint on the next lines
          breakTotalMs += STOP_MILLISECONDS;
          breakOn = breakTotalMs < breakMaxMs;
          // Set a breakpoint at the next line; you can also set breakOn=false to shut off breaking
          // in this jvm forever.
          //noinspection BusyWait
          Thread.sleep(STOP_MILLISECONDS);
        } while (breakOn);
      } else {
        log.warn("Not entering debuggerBreak, because debugging not enabled: {}", info);
        breakOn = false;
      }
    } catch (Exception e) {
      //noinspection CallToPrintStackTrace (debugging usage only)
      e.printStackTrace();
    }
    Breadcrumbs.send(
        PropertiesCrumb.apply(
            ChainedID$.MODULE$.root(),
            source,
            Properties.debug().apply(info),
            Properties.logMsg().apply("Leaving debuggerBreak")));
  }

  // returns true if debugging is actually enabled
  public static boolean sendDebugInfo(String info) throws IOException {
    // Looking for JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,address=${dbgport},server
    // and parsing debug port out
    // The file name created will host.debugPort.txt
    java.util.Properties vmProps = VMSupport.getAgentProperties();
    String dtsock = "dt_socket:";
    String address = vmProps.getProperty("sun.jdwp.listenerAddress");
    if (address != null && address.startsWith(dtsock)) port = address.substring(dtsock.length());
    else port = null;
    String portString = port == null ? "none" : port;

    List<String> inputArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
    String process = PThreadContext.Process.current().toString();
    String host = InetAddress.getLocalHost().getHostName();
    String jvmName = ManagementFactory.getRuntimeMXBean().getName();

    if (OPTIMUS_DBG_DIR != null) {
      File f = new File(Paths.get(OPTIMUS_DBG_DIR, process + "_" + portString + ".txt").toString());
      FileWriter fw = new FileWriter(f.getAbsolutePath());
      fw.write(jvmName);
      fw.write("\r\n");
      fw.write(inputArgs.toString());
      var env = System.getenv();
      for (var entry : env.entrySet()) {
        fw.write(entry.getKey() + " : " + entry.getValue() + "\r\n");
      }
      fw.close();
    }

    Elems elems =
        Properties.Elems$.MODULE$.apply(
            Properties.debug().apply(info),
            Properties.host().apply(host),
            Properties.port().apply(portString),
            Properties.name().apply(jvmName),
            Properties.args()
                .apply(CollectionConverters.iterableAsScalaIterable(inputArgs).toSeq()),
            Properties.logFile().apply(LoggingInfo.getLogFile()),
            Version$.MODULE$.verboseProperties());

    Breadcrumbs.send(PropertiesCrumb.apply(ChainedID$.MODULE$.root(), source, elems.m()));

    Breadcrumbs.flush();
    return port != null;
  }

  /**
   * Collect new Exceptions in a list on a method then use this method to process the stack traces
   * later.
   *
   * <p>Note - directly getting the current thread's stack trace in the method you're debugging
   * might be slow enough to eradicate the race you're chasing, hence using Exceptions here.
   *
   * @param stacks collected exceptions
   * @param upto the method name you want to print up to (useful for avoiding diffs caused by
   *     earlier frames you aren't interested in - drainQueue is a good one to use)
   */
  @SuppressWarnings("unused")
  public static String stacksString(ArrayList<Exception> stacks, String upto) {
    HashMap<String, Integer> setStacks = new HashMap<>();
    StringBuilder sb = new StringBuilder();
    for (Exception e : stacks) {
      String sStackTrace = stackAsString(e.getStackTrace(), upto);
      Integer count = setStacks.getOrDefault(sStackTrace, 0);
      setStacks.put(sStackTrace, count + 1);
    }

    ArrayList<Map.Entry<String, Integer>> results = new ArrayList<>(setStacks.entrySet());
    results.sort(Map.Entry.comparingByValue()); // for easier diffing
    for (Map.Entry<String, Integer> e : results) {
      sb.append("\nInstances ").append(e.getValue()).append("\n");
      sb.append(e.getKey());
    }
    return sb.toString();
  }

  @SuppressWarnings("unused")
  public static String stacksString() {
    return stacksString(Stacks, "");
  }

  public static final ArrayList<Exception> Stacks = new ArrayList<>();

  @SuppressWarnings("unused")
  public static void snapStack() {
    synchronized (Stacks) {
      Stacks.add(new Exception());
    }
  }

  private static String stackAsString(StackTraceElement[] ste, String upto) {
    StringBuilder sb = new StringBuilder();
    for (StackTraceElement te : ste) {
      sb.append(te.toString()).append("\n");
      if (!upto.isEmpty() && te.toString().contains(upto)) break;
    }
    return sb.toString();
  }

  private static final String InstancePropertyTargetClsName =
      InstancePropertyTarget.class.getName();
  private static final String PredicatedPropertyTweakTargetClsName =
      PredicatedPropertyTweakTarget.class.getName();
  private static final String PropertyTweakTargetClsName = PropertyTarget.class.getName();
  private static final String TwkPropertyInfo0Name = TwkPropertyInfo0.class.getName();
  private static final String TweakPropertyInfo =
      TwkPropertyInfo0Name.substring(0, TwkPropertyInfo0Name.length() - 1);

  private static final String TweakClsName = Tweak.class.getName();

  public static StackTraceElement tweakCreationSite() {
    return StackWalker.getInstance()
        .walk(
            stream -> {
              var seenTargetClass = false;
              var seenTweakConstructor = false;
              StackTraceElement beforeTweakConstructorFrame = null;
              var it = stream.iterator();
              while (it.hasNext()) {
                var frame = it.next();
                var clsName = frame.getClassName();

                if (seenTweakConstructor) {
                  // the frame above us was a Tweak frame to remember the one before it
                  beforeTweakConstructorFrame = frame.toStackTraceElement();
                  seenTweakConstructor = false;
                }

                if (clsName.equals(InstancePropertyTargetClsName)
                    || clsName.equals(PredicatedPropertyTweakTargetClsName)
                    || clsName.equals(PropertyTweakTargetClsName)) seenTargetClass = true;
                else if (clsName.equals(TweakClsName)) seenTweakConstructor = true;
                else if (seenTargetClass && !clsName.startsWith(TweakPropertyInfo))
                  return frame.toStackTraceElement();
              }
              // The best we can do :) return first tweak creation site in this stack
              return beforeTweakConstructorFrame;
            });
  }

  public static boolean between(long start, long end, long value) {
    return start <= value && end >= value;
  }
}
