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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

/**
 * @see MilliInstant
 */
public class MilliZonedDateTime {
  public static ZonedDateTime now() {
    return ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS);
  }

  public static ZonedDateTime now(Clock clock) {
    return ZonedDateTime.now(clock).truncatedTo(ChronoUnit.MILLIS);
  }

  public static ZonedDateTime now(ZoneId zone) {
    return ZonedDateTime.now(zone).truncatedTo(ChronoUnit.MILLIS);
  }
}
