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
package optimus.platform.inputs.registry.parameters;

import optimus.platform.inputs.NodeInput;
import optimus.platform.inputs.dist.DistNodeInputs;
import optimus.platform.inputs.registry.CombinationStrategies;
import optimus.platform.inputs.registry.Source;

public class GraphInputs {

  public static class SourceNames {
    public static final String EmitEntityRefStats = "optimus.dsi.enableEmitEntityReferenceStats";
  }

  public static final NodeInput<Boolean> EmitEntityRefStats =
      DistNodeInputs.newAutoForwardingOptimusDistJavaProperty(
          "EmitEntityRefStats",
          "PROBABLY CAN REMOVE!!!",
          Source.fromBoolJavaProperty(SourceNames.EmitEntityRefStats),
          Object::toString,
          CombinationStrategies.graphCombinator());

  public static final NodeInput<Boolean> DetectStalls =
      DistNodeInputs.newAutoForwardingOptimusDistJavaProperty(
          "DetectStalls",
          "Enables detection of Graph stalls",
          Source.fromBoolJavaProperty("optimus.graph.detectStalls"),
          Object::toString,
          CombinationStrategies.graphCombinator());

  public static final NodeInput<Boolean> DetectStallAdapted =
      DistNodeInputs.newAutoForwardingOptimusDistJavaProperty(
          "DetectStallAdapted",
          "Enables detection of Graph stalls on adapted nodes",
          Source.fromBoolJavaProperty("optimus.graph.detectStallAdapted"),
          Object::toString,
          CombinationStrategies.graphCombinator());

  public static final NodeInput<Integer> DetectStallAdaptedTimeout =
      DistNodeInputs.newAutoForwardingOptimusDistJavaProperty(
          "DetectStallAdaptedTimeout",
          "Timeout for Graph stalls on adapted nodes (in seconds)",
          Source.fromIntJavaProperty("optimus.graph.detectStallAdaptedTimeoutSecs"),
          Object::toString,
          CombinationStrategies.graphCombinator());

  public static final NodeInput<Integer> DetectStallInterval =
      DistNodeInputs.newAutoForwardingOptimusDistJavaProperty(
          "DetectStallInterval",
          "Interval to check for graph stalls (in seconds)",
          Source.fromIntJavaProperty("optimus.graph.detectStallIntervalSecs"),
          Object::toString,
          CombinationStrategies.graphCombinator());

  public static final NodeInput<Integer> DetectStallTimeout =
      DistNodeInputs.newAutoForwardingOptimusDistJavaProperty(
          "DetectStallTimeout",
          "Timeout for Graph stalls (in seconds)",
          Source.fromIntJavaProperty("optimus.graph.detectStallTimeoutSecs"),
          Object::toString,
          CombinationStrategies.graphCombinator());
}
