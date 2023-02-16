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
package patch;

import java.time.Clock;
import java.time.Instant;

public class MilliInstant {
  // TODO (OPTIMUS-33822): Delete all this when we know that > ms precision in Instant will not
  // break the DAL.

  /** Like java.time.now() but with precision limited to milliseconds */
  public static Instant now() {
    return Instant.ofEpochMilli(Clock.systemUTC().millis());
  }

  /** Like java.time.now(Clock), but with precision limited to milliseconds. */
  public static Instant now(Clock clock) {
    return Instant.ofEpochMilli(clock.millis());
  }
}
