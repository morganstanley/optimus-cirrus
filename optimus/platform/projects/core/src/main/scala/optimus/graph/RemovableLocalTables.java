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

import java.util.function.Consumer;
import optimus.core.IndexedArrayList;
import optimus.graph.diagnostics.EvictionReason;
import optimus.graph.diagnostics.sampling.Cardinality;
import optimus.graph.diagnostics.sampling.KnownStackRecorders;
import optimus.graph.diagnostics.sampling.StackRecorder;
import scala.Enumeration;

/**
 * So far only one class implements RemovableLocalTables, we should revisit some class hierarchy if
 * this changes. (Note though that static expungeSink needs only to contain contents of this
 * superclass.)
 */
public class RemovableLocalTables extends IndexedArrayList.IndexedItem {
  public final EnumCounter<EvictionReason> evictionCounter =
      new EnumCounter<>(EvictionReason.class);
  public final PluginType.PluginTracker pluginTracker = new PluginType.PluginTracker();

  public final KnownStackRecorders knownStackRecorders = new KnownStackRecorders();

  public final Cardinality.Counters cardinalities = new Cardinality.Counters();

  public final StackRecorder stackRecorder(Enumeration.Value k) {
    return knownStackRecorders.apply(k);
  }

  public final StackRecorder stackRecorder(int i) {
    return knownStackRecorders.apply(i);
  }

  public Cardinality.Counters getCardinalities() {
    return cardinalities.snap();
  }

  private static final RemovableLocalTables expungeSink = new RemovableLocalTables();

  protected static void incorporateExpunged(RemovableLocalTables expungee) {
    expungeSink.evictionCounter.add(expungee.evictionCounter);
    expungeSink.pluginTracker.accumulate(expungee.pluginTracker, 1);
    expungeSink.cardinalities.add(expungee.cardinalities);
    expungee.knownStackRecorders.expungeTo(expungeSink.knownStackRecorders);
  }

  protected static void applyToExpunged(Consumer<RemovableLocalTables> process) {
    process.accept(expungeSink);
  }
}
