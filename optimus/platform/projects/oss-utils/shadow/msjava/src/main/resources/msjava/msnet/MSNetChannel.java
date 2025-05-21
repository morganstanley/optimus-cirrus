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

package msjava.msnet;
import java.nio.channels.SelectionKey;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Throwables;
import msjava.msnet.utils.MSNetConfiguration;
public class MSNetChannel {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MSNetChannel.class);
    
    protected final AtomicInteger ops = new AtomicInteger();
    protected final MSNetTCPSocket socket;
    protected final String name;
    protected MSNetChannelListener readListener;
    protected MSNetChannelListener writeListener;
    protected MSNetChannelListener acceptListener;
    protected MSNetChannelListener connectListener;
    
    protected MSNetChannelEvent event;
     
    
    public MSNetChannel(String name, MSNetTCPSocket socket) {
        this.socket = socket;
        this.name = name;
        updateMyEvent();
    }
    public void enable(MSNetChannelOpertaion op) {
        for (;;) {
            int current = ops.get();
            int updated = current | op.selectionKey;
            if (current == updated){
                return;
            }
            if (ops.compareAndSet(current, updated))
                return;
        }
    }
    public void disable(MSNetChannelOpertaion op) {
        for (;;) {
            int current = ops.get();
            int updated = current & ~op.selectionKey;
            if (current == updated){
                return;
            }
            if (ops.compareAndSet(current, updated))
                return;
        }
    }
    public int getOps() {
        return ops.get();
    }
    public void setOps(int op) {
        ops.set(op);
    }
    public boolean isEnabled(MSNetChannelOpertaion op) {
        return (ops.get() & op.selectionKey) != 0;
    }
    
    public void setReadListener(MSNetChannelListener listener) {
        readListener = listener;
    }
    public void setWriteListener(MSNetChannelListener listener) {
        writeListener = listener;
    }
    public void setConnectListener(MSNetChannelListener listener) {
        connectListener = listener;
    }
    public void setAcceptListener(MSNetChannelListener listener) {
        acceptListener = listener;
    }
    public MSNetTCPSocket getSocket() {
        return socket;
    }
    
    public String getName() {
        return name;
    }
    
    
    void updateMyEvent() {
        MSNetLoop loop;
        if (event == null) {
            loop = new MSNetLoop(null);
        } else {
            loop = event.getLoop();
        }
        event = new MSNetChannelEvent(this, name, loop);
    }
    
    public synchronized void process(int readyOps_) {
        
        
        
        
        
        try {
            if ((readyOps_ & SelectionKey.OP_WRITE) != 0 && writeListener != null) {
                writeListener.channelReady(event);
            }
            if ((readyOps_ & SelectionKey.OP_READ) != 0 && readListener != null) {
                readListener.channelReady(event);
            }
            if (acceptListener != null && (readyOps_ & SelectionKey.OP_ACCEPT) != 0) {
                acceptListener.channelReady(event);
            }
            if (connectListener != null && (readyOps_ & SelectionKey.OP_CONNECT) != 0) {
                connectListener.channelReady(event);
            }
        } catch (Exception x) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Exception caught in channel callback. Channel: (" + this + ")", x);
            }
            
            if (MSNetConfiguration.propagateChannelCallbackExceptions()) {
                Throwables.throwIfUnchecked(x);
                throw new RuntimeException("Uncaught exception from channel callback. Channel: (" + this + ")", x);
            }
        }
    }
    public String toString() {
        return "MSNetChannel[" + name + ", " + MSNetChannelOpertaion.getOpsString(ops.get()) + "]";
    }
}
