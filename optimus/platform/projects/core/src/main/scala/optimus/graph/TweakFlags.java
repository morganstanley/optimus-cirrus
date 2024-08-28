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

public final class TweakFlags {
  public static final int none = 0;
  public static final int alwaysInvalidateTrack = 1;
  public static final int dupAllowed = 2;
  public static final int keepLazyEval = 4;
  public static final int withPrivateCacheOnUse = 8;
  public static final int internal = 16;
}
