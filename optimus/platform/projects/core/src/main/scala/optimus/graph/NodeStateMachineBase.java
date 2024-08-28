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

public class NodeStateMachineBase {
  public OGSchedulerContext _schedulerContext;

  /** call to this method is generated in NodeFutureSystem */
  public final void setEC(OGSchedulerContext ec) {
    _schedulerContext = ec;
  }

  final OGSchedulerContext getEC() {
    return _schedulerContext;
  }

  /** We want to reset and not hold on EC which is thread specific! */
  @SuppressWarnings(
      "WeakerAccess") // can be package private, but IJ for some reason flags it as an error in
  // usage
  public final OGSchedulerContext getAndResetEC() {
    OGSchedulerContext ec = _schedulerContext;
    _schedulerContext = null;
    return ec;
  }
}
