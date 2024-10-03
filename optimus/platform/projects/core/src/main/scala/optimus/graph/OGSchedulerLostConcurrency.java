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
package optimus.graph;

import static optimus.graph.EvaluationState.IN_NON_CONCURRENT_SEQ;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import optimus.core.MonitoringBreadcrumbs;
import optimus.graph.diagnostics.NodeName;
import optimus.graph.diagnostics.OGSchedulerNonConcurrentLoopKey;
import optimus.graph.diagnostics.gridprofiler.GridProfiler;
import optimus.graph.diagnostics.gridprofiler.GridProfiler$;
import optimus.graph.diagnostics.messages.SyncStackCounter$;
import optimus.graph.diagnostics.messages.SyncStackEvent;
import optimus.platform.util.PrettyStringBuilder;
import optimus.platform.ScenarioStack;

public class OGSchedulerLostConcurrency {
  private static final String LEGACY_SYNC_STACK_GREP_TARGET = "Stack to be asynced\n";
  private static final long MIN_LOST_OF_CONCURRENCY_TO_REPORT = 1_000_000; // 1ms
  // only keep top 10 entries for profiler html report
  private static final int TOP_N_LOST_CC_ENTRIES = 10;

  static PluginType uniqueCountPluginType = PluginType$.MODULE$.apply("UNIQUECOUNT");

  public abstract static class UniqueCounts extends AtomicInteger implements Serializable {
    public static final int SYNC_STACK_CRUMB_THRESHOLD = 1000;
    public int threshold = 1; // To support multiple warning with back-off logic
    public PluginType pluginType = uniqueCountPluginType;
    public String aggregationKey;
    public final String tpe;

    UniqueCounts(String tpe) {
      this.tpe = tpe;
    }

    UniqueCounts(int count, String tpe, PluginType pluginType) {
      super(count);
      this.tpe = tpe;
      this.pluginType = pluginType;
    }

    protected abstract UniqueCounts combineSameType(UniqueCounts other);

    public final UniqueCounts combine(UniqueCounts other) {
      if (other.tpe.equals(tpe)) return combineSameType(other);
      else {
        OGScheduler.log.warn("Tried to combine {} with {}, returning this", other, this);
        return this;
      }
    }

    @Override
    public String toString() {
      return String.format("tpe = %s count = %d pluginType = %s", tpe, get(), pluginType.name());
    }

    public void setKey(String key) {
      aggregationKey = key;
    }

    public abstract String displayString();
  }

  public static class CriticalSyncStack extends UniqueCounts {
    public static final String tpe = "css";

    public CriticalSyncStack() {
      super(tpe);
    }

    private CriticalSyncStack(int count, PluginType pluginType) {
      super(count, tpe, pluginType);
    }

    @Override
    protected CriticalSyncStack combineSameType(UniqueCounts other) {
      return new CriticalSyncStack(
          get() + other.get(), pluginType == PluginType.None() ? other.pluginType : pluginType);
    }

    @Override
    public String displayString() {
      return "Critical Sync Stack";
    }
  }

  /* Counts the number of adapted calls seen, to report only if seen > 1 */
  public static class NonConcurrentLoopDesc extends UniqueCounts {
    public static final String tpe = "ncl";
    // While collecting the information we don't want look/resolve names
    transient Class<?> iterationType;
    private String nameAndSource;

    public NonConcurrentLoopDesc() {
      super(tpe);
    }

    private NonConcurrentLoopDesc(int count, PluginType pluginType) {
      super(count, tpe, pluginType);
    }

    @Override
    protected NonConcurrentLoopDesc combineSameType(UniqueCounts other) {
      return new NonConcurrentLoopDesc(
          get() + other.get(), pluginType == PluginType.None() ? other.pluginType : pluginType);
    }

    String getNameAndSource() {
      if (nameAndSource == null && iterationType != null)
        nameAndSource = NodeName.nameAndSource(iterationType);
      return nameAndSource;
    }

    /** lazy initialisation of nameAndSource */
    private void writeObject(ObjectOutputStream out) throws IOException {
      getNameAndSource();
      out.defaultWriteObject();
    }

    @Override
    public String displayString() {
      return "Non-Concurrent Loop";
    }
  }

  private static final ConcurrentHashMap<String, UniqueCounts> uniqueStrings =
      new ConcurrentHashMap<>();

  private static final OGSchedulerNonConcurrentLoopKey nonConcurrentLoopKey =
      new OGSchedulerNonConcurrentLoopKey();

  /**
   * Only set IN_NON_CONCURRENT_SEQ flag if observer reports lost concurrency (which means we don't
   * need to check the observer type in later methods that read the flag)
   */
  static void updateScenarioStackForReporting(NodeTask seqTask) {
    // report outer foldLeft only
    if (!OGTrace.observer.recordLostConcurrency()
        || seqTask.scenarioStack().hasFlag(IN_NON_CONCURRENT_SEQ)) return;
    NonConcurrentLoopDesc ncld = new NonConcurrentLoopDesc();
    ScenarioStack ss =
        seqTask.scenarioStack().withPluginTag(IN_NON_CONCURRENT_SEQ, nonConcurrentLoopKey, ncld);
    seqTask.replace(ss);
  }

  /** Note: under lock */
  static void reportFirstIteration(NodeTask ntsk, int maxConcurrency) {
    ScenarioStack ss = ntsk.scenarioStack();
    if (maxConcurrency == 1 && ss.hasFlag(IN_NON_CONCURRENT_SEQ)) {
      ss.findPluginTag(nonConcurrentLoopKey)
          .foreach(
              (ncld) -> {
                // don't overwrite if set => this reports only outer foldLeft in nested case
                if (ncld.iterationType == null) ncld.iterationType = ntsk.getClass();
                return null;
              });
    }
  }

  static void reportPossibleLostConcurrency(NodeTask ntsk) {
    PluginType pluginType = ntsk.getReportingPluginType();
    if (ntsk.scenarioStack().hasFlag(IN_NON_CONCURRENT_SEQ)) {
      ntsk.scenarioStack()
          .findPluginTag(nonConcurrentLoopKey)
          .foreach(
              (ncld) -> {
                int newCount = ncld.incrementAndGet();
                // Consider: distinguishing by iteration (ensure different iteration)

                if (newCount
                    == 2) // don't report if only one iteration made a call to an adapted node
                report(ntsk, pluginType, ncld.getNameAndSource(), ncld);
                return null;
              });
    }
  }

  static void reportCriticalSyncStack(NodeTask ntsk, PluginType stalledOn, long waitTime) {

    if (!OGTrace.observer.recordLostConcurrency()) return;
    if (stalledOn == PluginType.None() && waitTime < MIN_LOST_OF_CONCURRENCY_TO_REPORT) return;

    String syncTrace = syncEntryAsString(Thread.currentThread().getStackTrace());
    if (!syncTrace.isEmpty()) report(ntsk, stalledOn, syncTrace, new CriticalSyncStack());
  }

  private static void report(
      NodeTask ntsk, PluginType stalledOn, String desc, UniqueCounts defaultCounts) {
    // In order NOT to dump the same stack all over, this set will be used to reduce the output
    // dump.
    // intentionally deduplicate on stack trace, not combined stack trace + node trace
    // otherwise things could be very verbose - you'd see every code path through which the sync
    // stack was hit
    // potentially this is useful information, maybe can control this via a flag if desired

    UniqueCounts counts = uniqueStrings.computeIfAbsent(desc, (String) -> defaultCounts);
    int newCount = counts.incrementAndGet();
    if (stalledOn != PluginType.None()) counts.pluginType = stalledOn;

    boolean isCriticalSS = counts.tpe.equals(CriticalSyncStack.tpe);
    if (OGTraceIsolation.isTracing() && isCriticalSS) {
      var name = ntsk.toPrettyName(true, false);
      SyncStackCounter$.MODULE$.publish(
          new SyncStackEvent(desc, 0, name, stalledOn.name(), newCount, true));
    }

    if (newCount == Settings.criticalSyncStackAlertThreshold && isCriticalSS)
      OGScheduler.log.warn("Hit awfulness threshold " + newCount + " for sync stack: " + desc);

    /* Note: we can record based on level, but will dump into logs only if dumpCriticalSyncStacks is set */
    if (Settings.dumpCriticalSyncStacks && isCriticalSS && newCount >= counts.threshold) {
      StringBuilder sb = new StringBuilder();
      NodeTask.NodeStackVisitor v =
          ntsk.waitersToNodeStack(
              false,
              new PrettyStringBuilder(sb),
              Settings.fullNodeTraceOnCriticalSyncStack,
              Settings.criticalSyncStackMaxSize);

      sb.append("\npaths=").append(v.paths).append(", maxdepth=").append(v.maxDepth);
      String legacyGrepTarget = newCount == 1 ? LEGACY_SYNC_STACK_GREP_TARGET : "";
      String message =
          "Critical >= " + counts.threshold + "\n" + legacyGrepTarget + desc + "Node Trace\n" + sb;
      OGScheduler.log.info(message);

      if (newCount >= UniqueCounts.SYNC_STACK_CRUMB_THRESHOLD) {
        MonitoringBreadcrumbs.sendCriticalSyncStackCrumb(desc, newCount);
      }

      counts.threshold *= 10;
    }

    // store in GridProfiler to be sent back over distribution and aggregated across engines
    GridProfiler$.MODULE$.recordLostConcurrency(desc, counts, ntsk);

    if (Settings.throwOnCriticalSyncStack && isCriticalSS)
      throw new GraphException("Critical sync stack " + ntsk + " " + desc);
  }

  /**
   * Only report the stack from the sync entry point (ie Node.get or PropertyNode.lookupAndGet), as
   * long as that entry point is not internal (ie, not in optimus.graph.OGScheduler*).
   */
  private static String syncEntryAsString(StackTraceElement[] stes) {
    StringBuilder sb = new StringBuilder();
    boolean haveSyncPoint = false;
    for (StackTraceElement e : stes) {
      if (haveSyncPoint) {
        if (!e.getClassName().startsWith("optimus.graph.OGScheduler")) {
          sb.append(e);
          sb.append("\n");
        } else break;
      } else if (e.getMethodName().endsWith("lookupAndGet")
          && e.getClassName().equals("optimus.graph.PropertyNode")) haveSyncPoint = true;
      else if (e.getMethodName().endsWith("get") && e.getClassName().equals("optimus.graph.Node"))
        haveSyncPoint = true;
    }
    return sb.toString();
  }

  static void checkSyncStacks(
      NodeTask ntsk, int currentCausalityID, OGSchedulerContext.SyncStack syncStack) {
    if (currentCausalityID != 0 && !ntsk.scenarioStack().ignoreSyncStacks()) {
      if (Settings.allowFailOnSyncStacks && ntsk.scenarioStack().failOnSyncStacks())
        throw new IllegalStateException(
            "Encountered sync node stack and failOnSyncStacks mode is set: " + syncStack);

      if (Settings.traceOnSyncStacks)
        OGScheduler.log.info("Encountered sync node stack: {}", syncStack);
    }
  }

  /**
   * { lost_cc: { css : [ { count: 10, plugin: "DAL", desc: "stack"}, { count: 10, plugin: "DAL",
   * desc: "stack"} ] ncl : [ { count: 10, plugin: "DAL", desc: "stack"}, { count: 2, plugin: "GSF",
   * desc: "stack"} ] } }
   */
  public static String reportAsJson(List<Map.Entry<String, UniqueCounts>> lostCCEntries) {
    if (lostCCEntries.isEmpty()) return "";
    HashMap<String, StringBuilder> mapSb = mainLostCCEntries(lostCCEntries);
    StringBuilder sb = new StringBuilder();
    sb.append("{ \"lost_cc\": {");
    appendTopLostCCFields(sb, mapSb);
    sb.append("\n}");
    return sb.toString();
  }

  private static HashMap<String, StringBuilder> mainLostCCEntries(
      List<Map.Entry<String, UniqueCounts>> entries) {
    entries.sort(Comparator.comparingInt(a -> a.getValue().get() * -1));
    HashMap<String, StringBuilder> mapSb = new LinkedHashMap<>();
    entries.forEach(
        entry -> {
          String desc = entry.getKey();
          UniqueCounts counts = entry.getValue();
          String type = counts.tpe;
          StringBuilder sb = mapSb.computeIfAbsent(type, s -> new StringBuilder());
          sb.append("{");
          sb.append(" \"tpe\": \"").append(type).append('\"').append(",");
          sb.append(" \"count\": ").append(counts.get()).append(",");
          sb.append(" \"plugin\": \"").append(counts.pluginType.name()).append('\"').append(",");
          sb.append(" \"key\": \"").append(counts.aggregationKey).append("\",");
          sb.append(" \"desc\": \"").append(desc.replace("\n", "\\n")).append('\"');
          sb.append(" },\n");
        });
    return mapSb;
  }

  public static String reportAsJs(List<Map.Entry<String, UniqueCounts>> lostCCEntries) {
    List<Map.Entry<String, UniqueCounts>> filteredEntries = topEntries(lostCCEntries);
    HashMap<String, StringBuilder> mapSb = mainLostCCEntries(filteredEntries);
    StringBuilder sb = new StringBuilder();
    sb.append("lost_cc = {");
    appendTopLostCCFields(sb, mapSb);
    return sb.toString();
  }

  private static void appendTopLostCCFields(
      StringBuilder lostCCStrBuilder, HashMap<String, StringBuilder> lostCCMap) {
    lostCCMap.forEach(
        (tpe, sbpart) -> {
          lostCCStrBuilder.append("\n").append("\"").append(tpe).append("\"").append(": [\n");
          sbpart.setLength(sbpart.length() - 2); // drop last comma and \n
          lostCCStrBuilder.append(sbpart.toString());
          lostCCStrBuilder.append("\n],\n");
        });
    if (!lostCCMap.isEmpty())
      lostCCStrBuilder.setLength(lostCCStrBuilder.length() - 2); // drop last comma and \n
    lostCCStrBuilder.append("\n}");
  }

  private static List<Map.Entry<String, UniqueCounts>> topEntries(
      List<Map.Entry<String, UniqueCounts>> entries) {
    // keep counts per tpe
    Map<String, Integer> perTpeCounts = new HashMap<>();
    List<Map.Entry<String, UniqueCounts>> filteredEntries = new ArrayList<>();
    for (Map.Entry<String, UniqueCounts> entry : entries) {
      String tpe = entry.getValue().tpe;
      if (perTpeCounts.containsKey(tpe)) {
        perTpeCounts.put(tpe, perTpeCounts.get(tpe) + 1);
      } else {
        perTpeCounts.put(tpe, 1);
      }
      if (perTpeCounts.get(tpe) <= TOP_N_LOST_CC_ENTRIES) {
        filteredEntries.add(entry);
      }
    }
    return filteredEntries;
  }

  public static List<Map.Entry<String, UniqueCounts>> lostCCEntries() {
    return GridProfiler.getLostConcurrencySummary();
  }
}
