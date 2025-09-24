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
package optimus.graph.diagnostics.trace;

import static optimus.graph.DiagnosticSettings.getDoubleProperty;
import static optimus.graph.DiagnosticSettings.getIntProperty;
import static optimus.graph.Settings.livePGO;
import static optimus.graph.Settings.livePGO_GivenNodes;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import com.fasterxml.jackson.databind.ObjectMapper;
import optimus.graph.NodeTask;
import optimus.graph.NodeTaskInfo;
import optimus.graph.OGLocalTables;
import optimus.graph.PropertyNode;
import optimus.graph.cache.NCPolicy;
import optimus.graph.diagnostics.gridprofiler.GridProfilerUtils;
import optimus.platform.EvaluationQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OGLivePGOObserver extends OGEventsNullObserver {
  private static final Logger log = LoggerFactory.getLogger(OGLivePGOObserver.class);
  private final ObjectMapper objectMapper = new ObjectMapper();
  private static final String name = "auto_optimizer";
  private static final String description =
      "<html>LivePGO is a performance optimization tool that automatically collects and analyzes performance data to improve the efficiency of applications. <br>It helps identify performance bottlenecks and suggests optimizations based on real-world usage patterns.</html>";

  private static final AtomicInteger cycleCount = new AtomicInteger(0);

  public static int livePGOIgnoreFirstCycles =
      getIntProperty("optimus.livePGO.ignoreFirstCycles", 0);

  public static int livePGOMinStarts = getIntProperty("optimus.livePGO.minStarts", 33);

  public static int livePGOGivenMinStarts = getIntProperty("optimus.livePGO.givenMinStarts", 10);

  public static double livePGODisableCacheRatio =
      getDoubleProperty("optimus.livePGO.disableCache.ratio", 0.001);

  public static double livePGODisableXSFTRatio =
      getDoubleProperty("optimus.livePGO.disableXSFT.ratio", 0.001);

  public static int livePGODumpConfigAfterCycles =
      getIntProperty("optimus.livePGO.dumpConfigAfterCycles", 0);

  // directory where to dump the collected PGO data (per cycle profiling)
  public static String livePGODataDir = System.getProperty("optimus.livePGO.dataDir");

  private static final String pgoDecisionJson = "pgo_decisions.json";

  public static int generateStatsEveryXCycles =
      getIntProperty("optimus.livePGO.generateStatsEveryXCycles", 0);

  public enum PGOTarget {
    GIVEN,
    REGULAR_NODE,
    CONSTRUCTOR_NODE
  }

  public static class Data implements Serializable {
    @Serial private static final long serialVersionUID = 1;
    private static final VarHandle vh_start;
    private static final VarHandle vh_hits;
    private static final VarHandle vh_completed;
    private static final VarHandle vh_trivialHits;
    int cycle;
    Data prevCycleData;
    String name;
    int hits;
    int trivialHits;
    int starts;
    int completed;
    boolean isCacheable;
    transient NodeTaskInfo info;

    PGOTarget dataType;

    public int getCycle() {
      return cycle;
    }

    public Data getPrevCycleData() {
      return prevCycleData;
    }

    public String getName() {
      return name;
    }

    public int getHits() {
      return hits;
    }

    public int getStarts() {
      return starts;
    }

    public int getCompleted() {
      return completed;
    }

    public boolean isCacheable() {
      return isCacheable;
    }

    public PGOTarget getDataType() {
      return dataType;
    }

    public NodeTaskInfo getInfo() {
      return info;
    }

    public boolean statsForGiven() {
      return dataType == PGOTarget.GIVEN;
    }

    private void getAndAddStarts(int i) {
      vh_start.getAndAdd(this, i);
    }

    private void getAndAddHits(int i) {
      vh_hits.getAndAdd(this, i);
    }

    private void getAndAddTrivialHits(int i) {
      vh_trivialHits.getAndAdd(this, i);
    }

    private void getAndAddCompleted(int i) {
      vh_completed.getAndAdd(this, i);
    }

    static {
      var lookup = MethodHandles.lookup();
      try {
        vh_start = lookup.findVarHandle(Data.class, "starts", int.class);
        vh_hits = lookup.findVarHandle(Data.class, "hits", int.class);
        vh_completed = lookup.findVarHandle(Data.class, "completed", int.class);
        vh_trivialHits = lookup.findVarHandle(Data.class, "trivialHits", int.class);
      } catch (NoSuchFieldException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    // clone constructor
    Data(Data d) {
      this.name = d.name;
      this.cycle = d.cycle;
      this.prevCycleData = d.prevCycleData;
      this.hits = d.hits;
      this.trivialHits = d.trivialHits;
      this.starts = d.starts;
      this.completed = d.completed;
      this.isCacheable = d.isCacheable;
      this.dataType = d.dataType;
    }

    public Data(NodeTaskInfo info, String name, boolean forceCacheable, PGOTarget targetType) {
      this.info = info;
      this.isCacheable = info.getCacheable() || forceCacheable;
      this.name = name;
      this.dataType = targetType;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  public static class PGODecision implements Serializable {
    public String name;
    public boolean disableCache;
    public boolean disableXSFT;

    public PGODecision(String name, boolean disableCache, boolean xsft) {
      this.name = name;
      this.disableCache = disableCache;
      this.disableXSFT = xsft;
    }
  }

  static class LookupStateWithPGOData extends LookupState {
    public Data data;

    public LookupStateWithPGOData(Data data, long startTime) {
      super(true, startTime);
      this.data = data;
    }
  }

  private volatile Data[] data = new Data[1024];

  public OGLivePGOObserver() {
    super();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String description() {
    return description;
  }

  private Data getData(NodeTaskInfo info, int id, NodeTask task, PGOTarget pgoTarget) {
    var data = this.data;
    if (id >= data.length || data[id] == null) {
      synchronized (this) {
        if (id >= data.length) {
          var newData = new Data[id + 1];
          System.arraycopy(data, 0, newData, 0, data.length);
          this.data = newData;
          data = newData;
        }
        if (data[id] == null) {
          var initAsCacheable = pgoTarget == PGOTarget.GIVEN;
          var name = task != null ? task.nodeName().toString() : info.name();
          var d = new Data(info, name, initAsCacheable, pgoTarget);
          data[id] = d;
          return d;
        }
      }
    }
    return data[id];
  }

  private Data getData(NodeTaskInfo info, PGOTarget pgoTarget) {
    return getData(info, info.profile, null, pgoTarget);
  }

  private Data getData(NodeTask task, PGOTarget pgoTarget) {
    return getData(task.executionInfo(), task.getProfileId(), task, pgoTarget);
  }

  private boolean isCacheControllable(NodeTask task) {
    return task.executionInfo().getCacheableNode();
  }

  @Override
  public void start(OGLocalTables lCtx, NodeTask task, boolean isNew) {
    super.start(lCtx, task, isNew);
    if (isNew && task.effectivelyCacheable()) {
      var data = getData(task, PGOTarget.REGULAR_NODE);
      data.getAndAddStarts(1);
    }
  }

  @Override
  public void completed(EvaluationQueue eq, NodeTask task) {
    super.completed(eq, task);
    if (isCacheControllable(task)) {
      var data = getData(task, PGOTarget.REGULAR_NODE);
      data.getAndAddCompleted(1);
    }
  }

  @Override
  public void lookupEnd(OGLocalTables lCtx, long startTime, NodeTask key, NodeTask lookupResult) {
    super.lookupEnd(lCtx, startTime, key, lookupResult);

    var underlying = lookupResult.cacheUnderlyingNode();
    // key == underlying means that the lookup returned what we've asked for (key) or key wrapped
    // into a proxy (therefore a miss). Note that eventually the proxy could be completed from
    // another node and that would result in a cache hit reported in lookupEndProxy
    if (key != underlying) {
      var trivialHit = key.computedInCacheID() == lookupResult.computedInCacheID();
      var data = getData(key, PGOTarget.REGULAR_NODE);
      data.getAndAddHits(1);
      if (trivialHit) data.getAndAddTrivialHits(1);
    }

    if (applyPGO()) {
      applyTrainedPGO(getData(key, PGOTarget.REGULAR_NODE));
    }
  }

  private boolean applyPGO() {
    return livePGO && cycleCount.get() >= livePGOIgnoreFirstCycles;
  }

  @Override
  public void lookupEndProxy(
      EvaluationQueue eq,
      PropertyNode<?> underlying,
      PropertyNode<?> proxy,
      boolean cacheHit,
      boolean countMiss) {
    super.lookupEndProxy(eq, underlying, proxy, cacheHit, countMiss);
    if (cacheHit && isCacheControllable(underlying)) {
      var data = getData(underlying, PGOTarget.REGULAR_NODE);
      data.getAndAddHits(1);
    }
  }

  @Override
  public void lookupAdjustCacheHit(EvaluationQueue eq, PropertyNode<?> hit) {
    super.lookupAdjustCacheHit(eq, hit);
  }

  /** Currently adjusts embeddable constructor cache stats */
  @Override
  public void lookupAdjustCacheStats(NodeTaskInfo nti, boolean hit, long startTime) {
    super.lookupAdjustCacheStats(nti, hit, startTime);
    var data = getData(nti, PGOTarget.CONSTRUCTOR_NODE);
    if (hit) data.getAndAddHits(1);
  }

  @Override
  public void markEndOfCycle() {
    var cycle = cycleCount.incrementAndGet();

    // only for debugging purposes we can dump the pgo decisions in json format
    if (generateStatsEveryXCycles != 0 && cycle % generateStatsEveryXCycles == 0) {
      List<PGODecision> decisionList = new ArrayList<>();
      for (Data d : this.data) {
        if (d != null && (disablesCache(d) || disablesXSFT(d))) {
          decisionList.add(new PGODecision(d.getName(), disablesCache(d), disablesXSFT(d)));
        }
      }
      saveToJsonFile(decisionList, livePGODataDir);
    }

    // after each cycle, maybe apply the trained PGO
    if (livePGO) {
      log.info("LIVE_PGO ON; cycle = " + cycle);
      if (cycle == livePGOIgnoreFirstCycles + 1) {
        applyTrainedPGO();
      }
    } else {
      // collect profiling data for PGO to be analyzed offline (aka training run)
      var data = this.data;
      for (Data d : data) {
        if (d != null && d.isCacheable) {
          d.prevCycleData = new Data(d);
          d.cycle = cycle;
        }
      }
    }

    // also write collected data but not when we run with live pgo enabled
    if (cycle == livePGODumpConfigAfterCycles) {
      saveToFile(livePGODataDir);
    }
  }

  public void saveToJsonFile(List<PGODecision> decisions, String outputDir) {
    var cycleDataFile = outputFilePath(outputDir, pgoDecisionJson);
    try {
      objectMapper.writeValue(new File(cycleDataFile), decisions);
      log.info("PGO Decision has been serialized to " + cycleDataFile);
    } catch (IOException e) {
      log.info("Error writing PGO decision to file: " + e.getMessage());
    }
  }

  @Override
  public LookupState lookupStartScenario(Object couldBeGiven) {
    // to avoid debugger markers we need to make sure this is a NodeTask
    if (couldBeGiven instanceof NodeTask task) {
      // currently given blocks don't get special execution info (it will be NodeTaskInfo.Default)
      if (task.executionInfo() == NodeTaskInfo.Default) {
        // important to initAsCacheable = true (start with given blocks being cacheable)
        // because NodeTask.Default is not cacheable
        // if given blocks had their own execution info, we could delete the initAsCacheable
        var data = getData(NodeTaskInfo.Given, task.getProfileId(), task, PGOTarget.GIVEN);
        if (!data.isCacheable) return LookupState.NoCache;
        return new LookupStateWithPGOData(data, lookupStart());
      }
    }
    return LookupState.Default;
  }

  @Override
  public void lookupAdjustCacheStats(LookupState ls, boolean hit) {
    if (ls instanceof LookupStateWithPGOData lse) {
      var givenData = lse.data;
      givenData.getAndAddStarts(1);
      if (hit) givenData.getAndAddHits(1);
      else if (applyPGO() && appliesForGiven(givenData) && disablesCacheForGiven(givenData)) {
        givenData.isCacheable = false;
      }
    }
  }

  public void applyTrainedPGO() {
    for (Data d : this.data) {
      if (d != null) applyTrainedPGO(d);
    }
  }

  private boolean appliesForGiven(Data d) {
    var shouldApplyPGO = d.dataType == PGOTarget.GIVEN && livePGO_GivenNodes;
    return shouldApplyPGO;
  }

  public void applyTrainedPGO(Data d) {
    // for given nodes we want to disable the cache only (not xsft)
    if (appliesForGiven(d) && disablesCacheForGiven(d)) {
      d.info.setCacheable(false);
    } else {
      if (disablesCache(d)) {
        d.info.setCachePolicy(NCPolicy.DontCache);
      } else if (disablesXSFT(d)) {
        d.info.setCachePolicy(NCPolicy.Basic);
      }
    }
  }

  public static boolean disablesCache(Data data) {
    if (data.starts < livePGOMinStarts) return false;

    var ratioMatches = data.starts * livePGODisableCacheRatio > data.hits;
    return data.starts > livePGOMinStarts && ratioMatches;
  }

  public static boolean disablesCacheForGiven(Data data) {
    if (data.starts < livePGOGivenMinStarts) return false;

    var wouldDisable = data.hits == 0 && data.starts > livePGOGivenMinStarts;
    return wouldDisable;
  }

  public static boolean disablesXSFT(Data data) {
    if (data.starts < livePGOMinStarts) return false;

    return data.hits * livePGODisableXSFTRatio > (data.hits - data.trivialHits);
  }

  private String outputFilePath(String outputDirName, String fName) {
    var cycleDataFile = outputDirName;

    if (Files.isDirectory(Path.of(outputDirName))) {
      cycleDataFile = outputDirName + File.separator + GridProfilerUtils.reportUniqueName() + fName;
    } else cycleDataFile = outputDirName + fName;

    return cycleDataFile;
  }

  public void saveToFile(String outputName) {
    // serialize the data and save it to a file
    var cycleDataFile = outputFilePath(outputName, name);
    try (FileOutputStream fileOut = new FileOutputStream(cycleDataFile);
        var objectOut = new ObjectOutputStream(fileOut)) {
      objectOut.writeObject(this.data); // Assuming `data` is serializable
      log.info("Data has been serialized to " + cycleDataFile);
    } catch (IOException e) {
      log.error("Error writing to file: " + e.getMessage());
    }
  }

  public Data[] loadFromFile(String fileName) {
    // load data from file in a serialized format
    try (var fileIn = new FileInputStream(fileName);
        var objectIn = new ObjectInputStream(fileIn)) {
      this.data = (Data[]) objectIn.readObject();
      log.info("Data has been deserialized from " + fileName);
    } catch (IOException | ClassNotFoundException e) {
      log.error("Error reading from file: " + e.getMessage());
    }
    return this.data;
  }

  public void dumpStats() {
    saveToFile(livePGODataDir);
  }

  /* TEST ONLY API */
  public void dbgSetCacheableFor(String name, Boolean cacheable) {
    for (Data datum : data) {
      var currentData = datum;
      while (currentData != null) {
        if (currentData.name.contains(name)) {
          currentData.info.setCacheable(cacheable);
        }
        currentData = currentData.prevCycleData;
      }
    }
  }
}
