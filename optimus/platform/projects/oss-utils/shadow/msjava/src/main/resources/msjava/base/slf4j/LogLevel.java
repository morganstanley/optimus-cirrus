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

package msjava.base.slf4j;
import org.slf4j.spi.LocationAwareLogger;
public enum LogLevel {
    TRACE(LocationAwareLogger.TRACE_INT),
    DEBUG(LocationAwareLogger.DEBUG_INT),
    INFO(LocationAwareLogger.INFO_INT),
    WARN(LocationAwareLogger.WARN_INT),
    ERROR(LocationAwareLogger.ERROR_INT);
    LogLevel(int value) {
        this.value = value;
    }
    private final int value;
    public final int getValue() {
        return value;
    }
}
