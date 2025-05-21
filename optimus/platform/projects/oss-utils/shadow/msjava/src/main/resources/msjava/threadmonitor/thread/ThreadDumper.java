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
package msjava.threadmonitor.thread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public interface ThreadDumper {
    Logger log = LoggerFactory.getLogger(ThreadDumper.class);
    
    String getAllStackTraces();
    
    String getStackTrace(Thread thread, int javaSysTid);
    static ThreadDumper init(boolean isUsingAsync, boolean isNativeMode) {
        log.info("Creating ThreadDumper with isUsingAsync: {} and isNativeMode: {}", isUsingAsync, isNativeMode);
        if (isUsingAsync && AsyncThreadDumper.isNatLibLoaded()) {
            try {
                return new AsyncThreadDumper(isNativeMode);
            } catch (IllegalStateException e) {
                
                log.error("Cannot use async profiler.", e);
            }
        }
        return new DefaultThreadDumper();
    }
}
