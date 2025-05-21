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

package msjava.msnet.utils;
import java.util.HashSet;
import java.util.Set;
import msjava.msnet.MSNetEvent;
import msjava.msnet.MSNetEventListener;
import msjava.msnet.MSNetLoop;
import msjava.msnet.MSNetTCPConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class IdleTimeoutScheduler implements MSNetEventListener, Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(IdleTimeoutScheduler.class);
    public static final long TIMEOUT_VALUE_NONE = -1;
    protected long _timeout = TIMEOUT_VALUE_NONE;
    private Object _timeoutCallbackId;
    private final MSNetLoop _netLoop;
    private final MSNetTCPConnection _connection;
    
    private Set<Long> currentWorkers = new HashSet<Long>();
    
    private volatile boolean closedByTimeoutScheduler = false;
    public IdleTimeoutScheduler(MSNetTCPConnection connection_) {
        this(connection_, connection_.getNetLoop());
    }
    public IdleTimeoutScheduler(MSNetTCPConnection connection_, MSNetLoop netloop_) {
        this._netLoop = netloop_;
        this._connection = connection_;
    }
    
    public void setTimeout(long timeoutDelay_) {
        _timeout = timeoutDelay_;
    }
    
    public long getTimeout() {
        return _timeout;
    }
    public boolean isTimeoutSet() {
        return _timeout != TIMEOUT_VALUE_NONE;
    }
    
    public synchronized void logWorkEnd() {
        if (isTimeoutSet()) {
            currentWorkers.remove(Thread.currentThread().getId());
            if (currentWorkers.isEmpty()) {
                resetTimeoutCallback();
            }
        }
    }
    
    public synchronized void logWorkStart() {
        if (isTimeoutSet()) {
            currentWorkers.add(Thread.currentThread().getId());
            cancelTimeoutCallback();
        }
    }
    public void eventOccurred(MSNetEvent e_) {
        run();
    }
    public void run() {
        if (_connection.isConnected()) {
            LOGGER.info("Closing connection due to idle timeout: {}", _connection.toString());
            closedByTimeoutScheduler = true;
            _connection.disconnect();
        }
    }
    
    public void resetTimeoutCallback() {
        if (_timeout != TIMEOUT_VALUE_NONE) {
            cancelTimeoutCallback();
            _timeoutCallbackId = _netLoop.callbackAfterDelay(_timeout, this);
        }
    }
    
    public void cancelTimeoutCallback() {
        if (_timeoutCallbackId != null) {
            _netLoop.cancelCallback(_timeoutCallbackId);
            _timeoutCallbackId = null;
        }
    }
    
    public void start() {
        if (_timeout != TIMEOUT_VALUE_NONE) {
            resetTimeoutCallback();
        }
    }
    public boolean isClosedByTimeoutScheduler() {
        return closedByTimeoutScheduler;
    }
}
