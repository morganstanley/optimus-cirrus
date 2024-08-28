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

import optimus.graph.NodeTask;
import optimus.graph.NodeTrace;
import optimus.graph.OGLocalTables;
import optimus.graph.OGTrace;
import optimus.graph.PTools;
import optimus.graph.Settings;

abstract class OGEventsGlobalGraphObserver extends OGEventsObserver {
  @Override
  public boolean requiresNodeID() {
    return false;
  }

  @Override
  public boolean writesDirectlyToTrace() {
    return false;
  }

  @Override
  public boolean collectsHotspots() {
    return false;
  }

  @Override
  public boolean recordLostConcurrency() {
    return Settings.dumpCriticalSyncStacks;
  }

  @Override
  public boolean traceTweaks() {
    return NodeTrace.traceTweaks.getValue();
  }

  @Override
  public boolean traceWaits() {
    return NodeTrace.traceWaits.getValue();
  }

  /**
   * We need the following overrides even in NullObserver because there is a possibility that if
   * mode is set to None and then back to, eg, hotspots, in between tests, we will end up with an
   * OGTrace.stop in hotspots WITHOUT first setting the current task on this context. This can cause
   * NPE in stop (depending on the mode).
   *
   * <p>The other way round also causes a problem - if we call stop in None then start in hotspots,
   * we won't have reset the task to null on the current context, and we'll treat this as a sync
   * entry point (thus messing up numbers)
   */
  @Override
  public void start(OGLocalTables lCtx, NodeTask task, boolean isNew) {
    lCtx.ctx.taskStart(task, 0);
  }

  @Override
  public void startAfterSyncStack(OGLocalTables lCtx, NodeTask task) {
    lCtx.ctx.taskStart(task, 0);
  }

  @Override
  public void stop(OGLocalTables lCtx, NodeTask task) {
    lCtx.ctx.resetRunningTask();
  }

  /**
   * 1. Called from OGScheduler.enterGraph() which is called from *user* entry point which is
   * runAndWait() 2. Called from OGScheduler.OGThread.run() when a new graph *helper* thread is
   * started. Also when "helper" thread re-enters scheduler (aka unparked from idle) It will likely
   * be followed by graphSpin/graphExit if work will not be found.
   */
  @Override
  public final void graphEnter(OGLocalTables lCtx) {
    PTools.registerCurrentThreadForSampling(lCtx.ctx);
    long time = OGTrace.nanoTime(); // Always required
    lCtx.ctx.graphEnter(time);
    graphEnter(lCtx, time);
  }

  protected void graphEnter(OGLocalTables lCtx, long time) {}

  @Override
  public final void graphExit(OGLocalTables lCtx) {
    long time = OGTrace.nanoTime(); // Always required
    lCtx.ctx.graphExit(time);
    graphExit(lCtx, time);
  }

  protected void graphExit(OGLocalTables lCtx, long time) {}

  @Override
  public final void graphEnterWait(OGLocalTables lCtx, int causalityID) {
    long time = OGTrace.nanoTime(); // Always required
    lCtx.ctx.waitStart(time);
    graphEnterWait(lCtx, causalityID, time);
  }

  protected void graphEnterWait(OGLocalTables lCtx, int causalityID, long time) {}

  @Override
  public final void graphExitWait(OGLocalTables lCtx) {
    long time = OGTrace.nanoTime(); // Always required
    lCtx.ctx.waitEnd(time);
    graphExitWait(lCtx, time);
  }

  protected void graphExitWait(OGLocalTables lCtx, long time) {}

  @Override
  public final void graphSpinEnter(OGLocalTables lCtx) {
    long time = OGTrace.nanoTime(); // Will be used in most cases
    lCtx.ctx.spinStart(time);
    graphSpinEnter(lCtx, time);
  }

  protected void graphSpinEnter(OGLocalTables lCtx, long time) {}

  @Override
  public final void graphSpinExit(OGLocalTables lCtx) {
    long time = OGTrace.nanoTime(); // Always required
    lCtx.ctx.spinEnd(time);
    graphSpinExit(lCtx, time);
  }

  protected void graphSpinExit(OGLocalTables lCtx, long time) {}
}
