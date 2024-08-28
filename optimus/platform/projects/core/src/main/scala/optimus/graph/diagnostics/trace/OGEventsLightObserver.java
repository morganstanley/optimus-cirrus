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
import optimus.graph.OGLocalTables;
import optimus.graph.OGTrace;
import optimus.graph.PThreadContext;

public class OGEventsLightObserver extends OGEventsGlobalGraphObserver {
  private static final String name = "light";
  private static final String description =
      "<html>Per thread graph counters are recorded<br>"
          + "Minimal impact on runtime and no impact on memory</html>";

  OGEventsLightObserver() {
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

  @Override
  public boolean recordLostConcurrency() {
    return true;
  }

  @Override
  public void start(OGLocalTables lCtx, NodeTask task, boolean isNew) {
    long time = OGTrace.nanoTime();
    PThreadContext ctx = lCtx.ctx;

    // This is the case of sync entry into another node
    if (ctx.ntsk != null) stop(lCtx, ctx.startTime, time);
    start(lCtx, task, time);
  }

  @Override
  public void startAfterSyncStack(OGLocalTables lCtx, NodeTask task) {
    // When a child node didn't run (e.g. it was already done) node is not restarting
    if (lCtx.ctx.ntsk == null) start(lCtx, task, OGTrace.nanoTime());
  }

  @Override
  public void stop(OGLocalTables lCtx, NodeTask task) {
    PThreadContext ctx = lCtx.ctx;
    long time = OGTrace.nanoTime();
    ctx.resetRunningTask();
    stop(lCtx, ctx.startTime, time);
  }

  private void start(OGLocalTables lCtx, NodeTask task, long time) {
    lCtx.ctx.taskStart(task, time);
  }

  private void stop(OGLocalTables lCtx, long startTime0, long stopTime) {
    long startTime = startTime0 > 0 ? startTime0 : stopTime;
    // currently context selfTime includes the whole period between start and stop (but should it?)
    lCtx.ctx.selfTime += stopTime - startTime;
    lCtx.ctx.nodeSuspendOrCompleteTime = 0;
  }

  public final long lookupStart() {
    return OGTrace.nanoTime();
  }

  @Override
  public void lookupEnd(OGLocalTables lt, long startTime, NodeTask task, NodeTask lookupResult) {
    long cacheTime = OGTrace.nanoTime() - startTime;
    lt.ctx.lookupEnd(cacheTime, task);
  }
}
