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
package optimus.utils;

public class ProcessExitCodes {

  public static final int OOM = 159;
  public static final int BAD_STATE = 193;
  public static final int XMX_PLUS_NATIVE_EXCEEDS_RSS = 194;
  public static final int EVICTED = 195; // cloud only
}
