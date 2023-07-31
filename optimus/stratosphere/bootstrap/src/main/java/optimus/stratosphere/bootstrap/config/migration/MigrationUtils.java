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
package optimus.stratosphere.bootstrap.config.migration;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class MigrationUtils {
  public static LocalDate getDateFrom(String dateStr) {
    if ("local".equals(dateStr)) {
      return LocalDate.now();
    }
    String updateDateCanonical =
        dateStr == null || "".equals(dateStr)
            ? "19000101"
            : dateStr.replace("-", "").replace(".", "").substring(0, 8);

    return LocalDate.parse(updateDateCanonical, DateTimeFormatter.ofPattern("yyyyMMdd"));
  }
}
