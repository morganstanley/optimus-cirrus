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
package msjava.base.slr.internal;
import java.util.EnumSet;
import msjava.base.slr.internal.ServiceLocatorClient.EventType;
public class NotificationPolicies {
    public static class EventTypeFilter implements NotificationPolicy, NotificationPolicy.NotificationCallback {
        private final EnumSet<EventType> pass;
        public EventTypeFilter(EventType... eventTypesToPass) {
            this.pass = EnumSet.noneOf(EventType.class);
            for (EventType et : eventTypesToPass) {
                pass.add(et);
            }
        }
        @Override
        public NotificationPolicy.NotificationCallback createCallback(ServiceLocatorClient cl) {
            return this;
        }
        @Override
        public NotificationNextStep onEvent(EventType eventType) {
            return pass.contains(eventType) ? NotificationPolicy.RUN : NotificationPolicy.STOP;
        }
    }
    
    public static class FrequencyLimiter implements NotificationPolicy {
        private NotificationPolicy delegate;
        private long shadowMs;
        public FrequencyLimiter(long shadowMs) {
            this(PASS_ALL_POLICY, shadowMs);
        }
        public FrequencyLimiter(NotificationPolicy delegate, long shadowMs) {
            this.delegate = delegate;
            this.shadowMs = shadowMs;
        }
        @Override
        public NotificationCallback createCallback(ServiceLocatorClient cl) {
            return new FrequencyLimiterCallback(delegate.createCallback(cl));
        }
        private class FrequencyLimiterCallback implements NotificationCallback {
            private final NotificationCallback callback;
            private volatile long nextOpen = Long.MIN_VALUE;
            public FrequencyLimiterCallback(NotificationCallback callback) {
                this.callback = callback;
            }
            @Override
            public NotificationNextStep onEvent(EventType eventType) {
                if (checkTimeoutUpdateNextOpen())
                    return callback.onEvent(eventType);
                else
                    return NotificationPolicy.STOP;
            }
            
            private boolean checkTimeoutUpdateNextOpen() {
                if (System.currentTimeMillis() > nextOpen) {
                    nextOpen = System.currentTimeMillis() + shadowMs;
                    return true;
                } else {
                    return false;
                }
            }
        }
    }
    public static final NotificationPolicy PASS_ALL_POLICY = new EventTypeFilter(EventType.values());
    public static final NotificationPolicy STOP_ALL_POLICY = new EventTypeFilter();
    public static final NotificationPolicy PASS_ONLY_UPDATES = new EventTypeFilter(EventType.UPDATE);
}
