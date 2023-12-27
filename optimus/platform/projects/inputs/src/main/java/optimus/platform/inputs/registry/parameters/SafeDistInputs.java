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

import optimus.platform.inputs.NodeInputs.ScopedSINodeInput;
import optimus.platform.inputs.dist.DistNodeInputs;
import optimus.platform.inputs.registry.CombinationStrategies;
import optimus.platform.inputs.registry.Source;

public class SafeDistInputs {

  // Optimus Distribution

  public static final ScopedSINodeInput<Boolean> SerializeClientRuntimeConfigForDist =
      DistNodeInputs.newOptimusSafeJavaOpt(
          "SerializeClientRuntimeConfigForDist",
          "I don't know!!!",
          Source.fromBoolJavaProperty("optimus.dsi.grid.useClientRuntimeConfig"),
          Object::toString);

  public static final ScopedSINodeInput<Boolean> OnBehalfDalSessionToken =
      DistNodeInputs.newOptimusSafeJavaOpt(
          "OnBehalfDalSessionToken",
          "I don't know!!!",
          Source.fromBoolJavaProperty("optimus.dsi.grid.useDalSessionToken"),
          Object::toString);

  public static final ScopedSINodeInput<Boolean> RealUserId =
      DistNodeInputs.newOptimusSafeJavaOpt(
          "RealUserId",
          "I don't know!!!",
          Source.fromBoolJavaProperty("optimus.dsi.grid.userRealUserId"),
          Object::toString);

  public static final ScopedSINodeInput<Boolean> UseZstd =
      DistNodeInputs.newOptimusSafeJavaOpt(
          "UseZstd",
          "I don't know!!!",
          Source.fromBoolJavaProperty("optimus.dist.useZstd"),
          Object::toString);
}
