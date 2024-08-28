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
import optimus.platform.EvaluationQueue;

public class OGEventsHotspotsWithHashesObserver extends OGEventsHotspotsObserver {
  private static final String name = "hashCounts";
  private static final String description =
      "<html>Same as hotspots but with child node costs and full node hash tracing</html>";

  OGEventsHotspotsWithHashesObserver() {
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
  public void completed(EvaluationQueue eq, NodeTask task) {
    super.completed(eq, task);

    OGLocalTables lt = OGLocalTables.getOrAcquire(eq);
    OGEventObserverUtils.writeHashes(lt.eventsTrace, null, task);
    lt.release();
  }
}
