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
package optimus.graph.diagnostics;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.opencsv.CSVWriter;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import optimus.core.TPDMask;
import optimus.graph.ConstructorNode;
import optimus.graph.SourceLocator;
import optimus.graph.NodeTask;
import optimus.graph.NodeTaskInfo;
import optimus.graph.PThreadContext;
import optimus.graph.cache.NCPolicy;
import optimus.graph.cache.NCSupport;
import optimus.graph.cache.NodeCCache;
import optimus.graph.diagnostics.trace.ReuseHistogram;

/**
 * Summary of execution for all NodeTasks that report executionInfo as the same NodeTaskInfo ~201
 * bytes
 */
public class PNodeTaskInfo extends PNodeTaskInfoLight {

  @Serial
  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    out.writeBoolean(nti != null);
  }

  @Serial
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    boolean hadNTI = in.readBoolean();
    if (hadNTI) {
      this.nti = new NodeTaskInfo(name + modifier, flags);
      this.id = this.nti.profile;
    }
  }

  /** Temporary PNTI used while executing */
  public PNodeTaskInfo(int id) {
    super(id);
    this.modifier = "";
  }

  /** Used by fully specified node */
  public PNodeTaskInfo(int id, NodeTask ntsk) {
    super(id);
    this.nti = ntsk.executionInfo();
    if (ntsk.isDynamic()) {
      // It would be much better to store just the clsID, but currently it's a trade-off
      // But currently we keep ntsk.getClass which is not great
      var nodeName = ntsk.nodeName();
      pkgName = nodeName.pkgName();
      name = nodeName.name();
      modifier = nodeName.modifier();
    } else {
      this.nodeCls = ntsk.getClass();
      this.modifier = "";
    }
  }

  /** Used from e.g. initAsCompleted where we don't have an executing task */
  public PNodeTaskInfo(int id, NodeTaskInfo nti) {
    super(id);
    this.nti = nti;
    this.modifier = "";
  }

  /** Used by vals that we want to instrument */
  public PNodeTaskInfo(NodeTaskInfo nti, String pkgName, String name) {
    super(nti.profile);
    this.nti = nti;
    this.pkgName = pkgName;
    this.name = name;
    this.modifier = nti.modifier();
    this.flags = nti.snapFlags();
  }

  /** Create completely un-attached PNodeTaskInfo */
  public PNodeTaskInfo(int id, String pkgName, String name, String modifier) {
    super(id);
    this.pkgName = pkgName;
    this.name = name;
    this.modifier = modifier;
  }

  public PNodeTaskInfo(int id, String pkgName, String name) {
    this(id, pkgName, name, "");
  }
  // For live profiling/debugging, to be able to update live code
  public transient NodeTaskInfo nti;
  // For live profiling/debugging, to be able to update live code
  public transient Class<?> nodeCls;

  public String pkgName; // package
  public String name; // class + property name
  // for 'modified' versions of an underlying original node eg. proxy, bootstrap
  public String modifier;

  public long flags; // Flags need to be updated at the last moment as they change
  public long wallTime; // Total Wall Time (Warning not-additive and therefore could be useless)
  // Mask is not updated inline, instead a new one is generated
  public TPDMask tweakDependencies = TPDMask.empty;
  public int tweakID;

  public int overInvalidated;
  public int reuseCycle;
  public long reuseStats;
  // specific to XSFT - top-level hits on the proxy are 'trivial'. If all hits are trivial, it's not
  // worth using xsft
  public int cacheHitTrivial;
  public String cacheName;
  public String cachePolicy;
  public String returnType;
  public NCSupport.JVMClassInfo jvmInfo;
  public transient int enqueuingProperty; // not meaningful for uncached nodes
  // populated on combine (ref to already transmitted aggregation key)
  public String enqueuingPropertyName;
  public ArrayList<PNodeTaskInfo> edges; // [EXPERIMENTAL_EDGES]

  /* Keeps distinct counts and we want to avoid allocating until someone will actually ask for those counts */
  public static class Distinct {
    static Distinct empty = new Distinct();

    public static class IntHashSetEx extends IntOpenHashSet {
      public int insertCount;

      public void addEx(int k) {
        insertCount++;
        super.add(k);
      }

      public void addAll(IntHashSetEx c) {
        super.addAll(c);
        insertCount += c.insertCount;
      }
    }

    public IntHashSetEx scenarioStacks = new IntHashSetEx();
    public IntHashSetEx entities = new IntHashSetEx();
    public IntHashSetEx args = new IntHashSetEx();
    public IntHashSetEx results = new IntHashSetEx();
    public IntHashSetEx combined = new IntHashSetEx();
    public IntHashSetEx IdScenarioStacks = new IntHashSetEx();
    public IntHashSetEx IdEntities = new IntHashSetEx();
    public IntHashSetEx IdArgs = new IntHashSetEx();
    public IntHashSetEx IdResults = new IntHashSetEx();
    public IntHashSetEx IdCombined = new IntHashSetEx();

    public void add(Distinct other) {
      if (other == null) return;
      scenarioStacks.addAll(other.scenarioStacks);
      entities.addAll(other.entities);
      args.addAll(other.args);
      results.addAll(other.results);
      combined.addAll(other.combined);
      IdScenarioStacks.addAll(other.IdScenarioStacks);
      IdEntities.addAll(other.IdEntities);
      IdArgs.addAll(other.IdArgs);
      IdResults.addAll(other.IdResults);
      IdCombined.addAll(other.IdCombined);
    }
  }

  public Distinct distinct;

  public Distinct getDistinct() {
    return distinct == null ? Distinct.empty : distinct;
  }

  public final NodeName nodeName() {
    return NodeName.apply(pkgName, name, modifier);
  }

  public PThreadContext.Process getProcess() {
    return PThreadContext.Process.none;
  }

  public final String fullName() {
    return NodeName.fullName(pkgName, name, modifier);
  }

  public final boolean getCacheable() {
    return NodeTaskInfo.getCacheable(flags);
  }

  public final boolean trackForInvalidation() {
    return NodeTaskInfo.trackForInvalidation(flags);
  }

  public final boolean isScenarioIndependent() {
    return NodeTaskInfo.isScenarioIndependent(flags);
  }

  public final boolean getFavorReuse() {
    return cachePolicy != null;
  }

  public final boolean getFavorReuseIsXS() {
    return Objects.equals(cachePolicy, NCPolicy.XS.policyName());
  }

  public final boolean isConstructor() {
    return name.endsWith(ConstructorNode.suffix());
  }

  public final String fullNamePackageShortened() {
    return NodeName.fullNamePackageShortened(pkgName, name, modifier);
  }

  /* Makes sense on PropertyNode only, aka ename */
  public final String entityName() {
    if (name == null) return "[NO ENTITY]";
    int lastDot = name.lastIndexOf('.');
    return pkgName
        + '.'
        + ((lastDot < 0) ? name + modifier : name.substring(0, lastDot) + modifier);
  }

  /* Makes sense on PropertyNode only, aka pname */
  public final String propertyName() {
    if (name == null) return "[NO PROPERTY]";
    int lastDot = name.lastIndexOf('.');
    return name.substring(lastDot + 1) + modifier;
  }

  public final String source() {
    if (nodeCls == null) return "unknown";
    else return SourceLocator.sourceOf(nodeCls);
  }

  public final String fullNameAndSource() {
    String name = fullName();
    if (nodeCls == null) return name;
    return name + " (" + SourceLocator.sourceOf(nodeCls) + ")";
  }

  /** Combination of cache policy and cache name useful for displaying as one column */
  public final String cacheDescription() {
    if (cachePolicy == null) return cacheName;
    if (cacheName == null) return "/" + cachePolicy;
    return "/" + cachePolicy + ":" + cacheName;
  }

  public final long nodeReusedTime() {
    return nodeUsedTime == 0 ? 0 : nodeUsedTime - ancAndSelfTime;
  }

  /**
   * Difference between time saved by caching and time taken to do cache lookups, aka net cache
   * benefit. Note - this is not the same as nodeReusedTime, which does not account for time spent
   * doing cache lookup. If cacheBenefit is negative then caching the node might not be a good idea
   * (pgo will determine this)
   */
  public final long cacheBenefit() {
    return nodeReusedTime() - cacheTime;
  }

  /** For display, reporting and tests */
  public final long ancSelfTime() {
    return ancAndSelfTime == -1 ? -1 : ancAndSelfTime - selfTime;
  }

  public final long avgCacheTime() {
    long cacheCnt = cacheHit + cacheMiss;
    return (0 == cacheCnt) ? 0 : cacheTime / cacheCnt;
  }

  public boolean hasNativeMarker() {
    return (flags & NodeTaskInfo.HOLDS_NATIVE) != 0;
  }

  public boolean hasSyncIdMarker() {
    return (flags & NodeTaskInfo.SINGLE_THREADED) != 0;
  }

  public boolean externallyConfiguredPolicy() {
    return (flags & NodeTaskInfo.EXTERNALLY_CONFIGURED_POLICY) != 0;
  }

  public boolean externallyConfiguredCache() {
    return (flags & NodeTaskInfo.EXTERNALLY_CONFIGURED_CUSTOM_CACHE) != 0;
  }

  public boolean profilerUIConfigured() {
    return (flags & NodeTaskInfo.PROFILER_UI_CONFIGURED) == NodeTaskInfo.PROFILER_UI_CONFIGURED;
  }

  public boolean isInternal() {
    return (flags & NodeTaskInfo.PROFILER_INTERNAL) == NodeTaskInfo.PROFILER_INTERNAL;
  }

  public boolean isProfilerProxy() {
    return (flags & NodeTaskInfo.PROFILER_PROXY) == NodeTaskInfo.PROFILER_PROXY;
  }

  public boolean dontTrackForInvalidation() {
    return !NodeTaskInfo.trackForInvalidation(flags);
  }

  public boolean wasTweaked() {
    return (flags & NodeTaskInfo.WAS_TWEAKED) != 0;
  }

  public final boolean isDirectlyTweakable() {
    return NodeTaskInfo.isDirectlyTweakable(flags);
  }

  public final boolean isGivenRuntimeEnv() {
    return NodeTaskInfo.isGivenRuntimeEnv(flags);
  }

  /**
   * Returns true if property was cache hit at least once and mask is not a poison mask (useless
   * info)
   */
  public final boolean hasCollectedDependsOnTweakMask() {
    return tweakDependencies != TPDMask.poison && cacheHit > 0;
  }

  /** test helper */
  public final boolean dependsOn(PNodeTaskInfo p) {
    return tweakDependencies.intersects(TPDMask.fromIndex(p.tweakID));
  }

  @Override
  public void reset() {
    super.reset();
    flags = 0L;
    wallTime = 0L;
    overInvalidated = 0;
    reuseCycle = 0;
    reuseStats = 0L;
    cacheHitTrivial = 0;
    returnType = null;
    cacheName = null;
    cachePolicy = null;
    distinct = null;
    tweakDependencies = TPDMask.empty;
    tweakID = 0;
  }

  // cache hits on combined row = hits of proxies + hits on owner, cache miss on combined row = miss
  // on owner,
  // cache node reused time on combined row = node reuse time of proxies + node reuse time of owner
  // proxy self time is attributed to postComplete of owner because it represents matchXScenario
  // time (so total self and
  // postComplete time will stay the same in aggregated mode - invariant)
  // cache time of proxy added to cache time of owner
  // for now, reuse cycle does not change [SEE_PROXY_REUSE_CYCLE]
  // cache misses stay the same (we just care about misses on owner)
  // same for evictions
  // same for invalidations and over-invalidations - the proxy is ttracked and invalidated, but not
  // the owner
  public void mergeWithProxy(PNodeTaskInfo proxy) {
    cacheHit += proxy.cacheHit;
    cacheHitTrivial += proxy.cacheHit;
    cacheHitFromDifferentTask += proxy.cacheHitFromDifferentTask;
    evicted += proxy.evicted;
    nodeUsedTime += proxy.nodeUsedTime;
    // Consider change: proxy.selfTime should be added to cache time, no caching -> no proxy time!
    postCompleteAndSuspendTime += proxy.selfTime + proxy.postCompleteAndSuspendTime;
    cacheTime += proxy.cacheTime;
    mergeDistinct(proxy);
  }

  private void mergeDistinct(PNodeTaskInfo other) {
    if (other.distinct != null) {
      if (distinct == null) distinct = new Distinct();
      distinct.add(other.distinct);
    }
  }

  // only selfTime matters here - bootstrap starts aren't stable, and it's non-cacheable so other
  // fields
  // don't need aggregation
  public void mergeWithBootstrap(PNodeTaskInfo bootstrap) {
    postCompleteAndSuspendTime += bootstrap.selfTime + bootstrap.postCompleteAndSuspendTime;
  }

  public PNodeTaskInfo dup() {
    return combine(new PNodeTaskInfo(0));
  }

  @Override
  public void merge(PNodeTaskInfoLight t) {
    super.merge(t);
    if (t instanceof PNodeTaskInfo tc) {
      // First "merge" fields that are really unique and identifying the PNodeTaskInfo
      nti = nti != null ? nti : tc.nti;
      nodeCls = nodeCls != null ? nodeCls : tc.nodeCls;
      returnType = returnType != null ? returnType : tc.returnType;
      jvmInfo = jvmInfo != null ? jvmInfo : tc.jvmInfo;
      tweakID = tweakID != 0 ? tweakID : tc.tweakID;
      enqueuingPropertyName =
          enqueuingPropertyName != null ? enqueuingPropertyName : tc.enqueuingPropertyName;
      enqueuingProperty = enqueuingProperty != 0 ? enqueuingProperty : tc.enqueuingProperty;

      if (cacheName == null) cacheName = tc.cacheName;

      if (cachePolicy == null) cachePolicy = tc.cachePolicy;

      if (tc.distinct != null) {
        if (distinct == null) distinct = new Distinct();
        distinct.add(tc.distinct);
      }

      if (tc.tweakDependencies != TPDMask.empty) {
        if (tweakDependencies == TPDMask.empty) tweakDependencies = new TPDMask();
        tweakDependencies.merge(tc.tweakDependencies);
      }

      flags |= tc.flags;
      overInvalidated += tc.overInvalidated;
      reuseCycle = Math.max(reuseCycle, tc.reuseCycle);
      reuseStats = ReuseHistogram.combine(reuseStats, tc.reuseStats);
      cacheHitTrivial += tc.cacheHitTrivial;
      wallTime += tc.wallTime;
    }
  }

  public PNodeTaskInfo combine(PNodeTaskInfoLight t) {
    PNodeTaskInfo tag = new PNodeTaskInfo(id, pkgName, name, modifier);
    tag.merge(this);
    tag.merge(t);
    return tag;
  }

  public PNodeTaskInfo freezeLiveInfo() {
    if (nti != null) {
      flags = nti.snapFlags();
      reuseCycle = nti.reuseCycle;
      reuseStats = nti.reuseStats;
      tweakID = nti.tweakableID();
      // ensure frozen version is a duplicate!
      tweakDependencies = nti.dependsOnTweakMask().dup();
      NodeCCache ccache = nti.customCache();
      if (ccache != null) cacheName = ccache.getName();
      // Default is just not interesting
      cachePolicy = nti.cachePolicy().policyName();
    }

    if (name == null) {
      NodeName nodeName = NodeName.from(nti, nodeCls);
      pkgName = nodeName.pkgName();
      name = nodeName.name();
      modifier = nodeName.modifier();
    }
    return this;
  }

  public final String flagsAsString() {
    return NodeTaskInfo.flagsAsString(flags);
  }

  public final String flagsAsStringVerbose() {
    return NodeTaskInfo.flagsAsStringVerbose(flags, cachePolicy);
  }

  @Override
  public String toString() {
    String wallTimeStr = String.format("%.4f", (wallTime * 1e-6));
    String selfTimeStr = String.format("%.4f", (selfTime * 1e-6));
    String cacheTimeStr = String.format("%.4f", (cacheTime * 1e-6));
    return fullName()
        + " "
        + flagsAsString()
        + " start: "
        + start
        + " wallTime: "
        + wallTimeStr
        + " selfTime: "
        + selfTimeStr
        + " cacheTime: "
        + cacheTimeStr
        + " cacheHit: "
        + cacheHit
        + " cacheMiss: "
        + " "
        + cacheMiss
        + " ";
  }

  @Override
  public int hashCode() {
    return ((name != null) ? name.hashCode() * 41 : 0)
        + ((pkgName != null) ? pkgName.hashCode() : 0)
        + ((modifier != null) ? modifier.hashCode() : 0);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PNodeTaskInfo t)) return false;
    return Objects.equals(name, t.name)
        && Objects.equals(pkgName, t.pkgName)
        && Objects.equals(modifier, t.modifier);
  }

  /** compare fields - for use in tests */
  @Override
  public boolean fieldsEqual(PNodeTaskInfoLight other) {
    if (this == other) return true;
    if (!equals(other)) return false;
    PNodeTaskInfo t = (PNodeTaskInfo) other; // remember .equals does a check for type first
    return super.fieldsEqual(other)
        && // compare underlying fields first
        flags == t.flags
        && wallTime == t.wallTime
        && overInvalidated == t.overInvalidated
        && reuseCycle == t.reuseCycle
        && reuseStats == t.reuseStats
        && cacheHitTrivial == t.cacheHitTrivial
        && Objects.equals(returnType, t.returnType)
        && Objects.equals(cacheName, t.cacheName)
        && Objects.equals(cachePolicy, t.cachePolicy)
        && distinct == t.distinct
        && Objects.equals(tweakDependencies, t.tweakDependencies)
        && tweakID == t.tweakID;
  }

  // print hotspots info, same sequence of columns as UNPTablePNodeTaskInfos
  // used by GridProfiler and TraceExplorer
  public static final class CSVHEADER {
    static final String ENGINE = "Engine";
    public static final String STARTED = "Node Started";
    public static final String EVICTIONS = "Evictions";
    static final String INVALIDATES = "Invalidates";
    public static final String HITS = "Cache Hits";
    public static final String MISSES = "Cache Misses";
    static final String CACHETIME = "Total Cache Time (ms)";
    static final String REUSEDTIME = "Node Reused Time (ms)";
    static final String BENEFIT = "Cache Benefit (ms)";
    static final String CACHETIMEAVG = "Avg Cache Time (ms)";
    static final String WALL = "Wall Time (ms)";
    static final String SELF = "Self Time (ms)";
    public static final String ANC = "ANC Self Time (ms)";
    public static final String POSTCOMPLETE = "Post complete and suspend time (ms)";
    public static final String NAME = "Property Name";
    static final String CACHEABLE = "Cacheable";
    public static final String SI = "isScenarioIndependent";
    public static final String XS = "favorReuse";
  }

  public static void printCSV(Map<String, List<PNodeTaskInfo>> agg, int filtered, Writer writer) {
    CSVWriter csvWriter = new CSVWriter(writer);
    String[] header = {
      CSVHEADER.ENGINE,
      CSVHEADER.STARTED,
      CSVHEADER.EVICTIONS,
      CSVHEADER.INVALIDATES,
      CSVHEADER.HITS,
      CSVHEADER.MISSES,
      CSVHEADER.CACHETIME,
      CSVHEADER.REUSEDTIME,
      CSVHEADER.BENEFIT,
      CSVHEADER.CACHETIMEAVG,
      CSVHEADER.WALL,
      CSVHEADER.SELF,
      CSVHEADER.ANC,
      CSVHEADER.POSTCOMPLETE,
      filtered > 0
          ? CSVHEADER.NAME + " (" + filtered + " properties filtered out)"
          : CSVHEADER.NAME,
      CSVHEADER.CACHEABLE,
      CSVHEADER.SI,
      CSVHEADER.XS
    };
    csvWriter.writeNext(header);
    for (String engine : agg.keySet()) {
      for (PNodeTaskInfo pnti : agg.get(engine)) {
        String[] row = {
          engine,
          "" + pnti.start,
          "" + pnti.evicted,
          "" + pnti.invalidated,
          "" + pnti.cacheHit,
          "" + pnti.cacheMiss,
          "" + (pnti.cacheTime * 1e-6),
          "" + (pnti.nodeReusedTime() * 1e-6),
          "" + (pnti.cacheBenefit() * 1e-6),
          "" + (pnti.avgCacheTime() * 1e-6),
          "" + (pnti.wallTime * 1e-6),
          "" + (pnti.selfTime * 1e-6),
          "" + (pnti.ancSelfTime() * 1e-6),
          "" + (pnti.postCompleteAndSuspendTime * 1e-6),
          pnti.fullName(),
          "" + pnti.getCacheable(),
          "" + pnti.isScenarioIndependent(),
          "" + pnti.getFavorReuse(),
        };
        csvWriter.writeNext(row);
      }
    }
  }
}
