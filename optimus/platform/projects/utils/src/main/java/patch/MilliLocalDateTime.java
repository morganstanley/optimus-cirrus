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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

/** @see MilliInstant */
public class MilliLocalDateTime {
  public static LocalDateTime now() {
    return LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
  }
  public static LocalDateTime now(Clock clock) {
    return LocalDateTime.now(clock).truncatedTo(ChronoUnit.MILLIS);
  }
  public static LocalDateTime now(ZoneId zone) {
    return LocalDateTime.now(zone).truncatedTo(ChronoUnit.MILLIS);
  }
}
