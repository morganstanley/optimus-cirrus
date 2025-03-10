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
package optimus.platform;

public class ScenarioFlags {
  public static final int none = 0;
  public static final int hasReducibleToByValueTweaks = 1;
  public static final int hasPossiblyRedundantTweaks = 1 << 1;
  public static final int disableRemoveRedundant = 1 << 2;
  public static final int hasUnresolvedOrMarkerTweaks = 1 << 3;
  public static final int hasContextDependentTweaks = 1 << 4;
  public static final int hasWhenClauseTweaks = 1 << 5;
  public static final int unorderedTweaks = 1 << 6;

  public static final int markedForDebugging = 1 << 7;
}
