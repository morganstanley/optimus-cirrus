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
import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import msjava.base.util.internal.SystemPropertyUtils;
import msjava.msnet.utils.MSNetConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class MSNetMultiplexer {
    
    public static interface WakeupListener {
        
        public void wakeupCallback();
    }
    private static final Logger LOGGER = LoggerFactory.getLogger(MSNetMultiplexer.class);
    private static final String NIO_BUG_MESSAGE = "You are using a version of JDK on linux AS 3 that is exposed to a known NIO bug.\n"
            + "By default selector CLOSE workaround is enabled, if you would like to use diffrent workaround or avoid this log message \n"
            + "please set -Dmsjava.msnet.fix.niospin={CLOSE|QUIT|WARN} JVM argument.\n"
            + "Refer http:
    enum NioSpinRecovery {
        CLOSE, QUIT, WARN;
    }
    static final int NIO_SPIN_MAX_COUNT = SystemPropertyUtils.getInteger("msjava.msnet.fix.niospin.maxcount", 1000,
            LOGGER);
    
    private static final int NIO_BUG_LOGGING_THRESHOLD = SystemPropertyUtils.getInteger(
            "msjava.msnet.fix.niospin.logthreshold", 50, LOGGER);
    static final NioSpinRecovery DEFAULT_NIO_SPIN_RECOVERY = NioSpinRecovery.CLOSE;
    static final NioSpinRecovery NIO_SPIN_RECOVERY;
    static final boolean BROKEN_NIO;
    static {
        
        boolean isJava6 = SystemPropertyUtils.getProperty("java.version", LOGGER).startsWith("1.6.");
        boolean isLinux = MSNetConfiguration.OS_IS_LINUX;
        boolean isAS3 = SystemPropertyUtils.getProperty("os.version", LOGGER).startsWith("2.4.21");
        BROKEN_NIO = isJava6 && isLinux && isAS3;
        
        String recoveryStr = SystemPropertyUtils.getProperty("msjava.msnet.fix.niospin", LOGGER);
        try {
            if (recoveryStr == null) {
                NIO_SPIN_RECOVERY = DEFAULT_NIO_SPIN_RECOVERY;
            } else {
                NIO_SPIN_RECOVERY = Enum.valueOf(NioSpinRecovery.class, recoveryStr.toUpperCase());
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Value set for -Dmsjava.msnet.fix.niospin (" + recoveryStr
                    + ") can only be WARN or CLOSE or QUIT");
        }
        
        if (BROKEN_NIO) {
            if (recoveryStr == null) {
                LOGGER.warn(NIO_BUG_MESSAGE);
            } else {
                LOGGER.info("The workaround for NIO spin is ENABLED with following value:" + NIO_SPIN_RECOVERY);
            }
        }
    }
    
    Selector _selector;
    
    MSNetMultiplexStatus _status;
    
    MSNetMultiplexer.WakeupListener _wakeupListener;
    volatile boolean _wokenUp;
    
    private static final Logger BUG_DETAILS_LOGGER = LoggerFactory.getLogger("msjava.msnet.NIOFixDetail");
    transient int nioBugSpinTracker;
    private transient long nioBugOccuranceCount = 1;
    private int nioBugLoggingModControl = 50; 
    
    public MSNetMultiplexer() {
        try {
            _selector = Selector.open();
        } catch (IOException x) {
            
            throw new MSNetFatalException("Could not open Selector", x);
        }
        _status = new MSNetMultiplexStatus();
    }
    public synchronized void close() {
        try {
            if (_selector != null) {
                _selector.close();
                _selector = null;
            }
            _wakeupListener = null;
        } catch (IOException x) {
            LOGGER.warn("Could not close Selector: {}", x.toString());
        }
    }
    public void register(MSNetChannel channel) throws MSNetException {
        register(_selector, channel);
    }
    private void register(Selector sel, MSNetChannel channel) throws MSNetException {
        if (channel == null)
            return;
        if (sel == null)
            return;
        SelectableChannel sc = channel.getSocket().getSelectableChannel();
        if (sc == null)
            return;
        try {
            sc.register(sel, channel.getOps(), channel);
        } catch (CancelledKeyException e) {            
            throw new MSNetSocketEndPipeException("Tried to register a socket whose key is canceled: "+  channel, e);
        } catch (ClosedChannelException e) {
            throw new MSNetSocketEndPipeException("Tried to register a socket that has been closed: "+  channel, e);
        } catch (Exception e) {
            throw new MSNetException("Tried to register a socket channel on a closed multiplexer: [selector="
                    + _selector + "]", e);
        }
    }
    public void register(MSNetTCPSocket socket, int op) throws MSNetException {
        assert op != 0;
        try {
            SelectableChannel sc = socket.getSelectableChannel();
            int allOps;
            SelectionKey key = sc.keyFor(_selector);
            if (key == null) {
                allOps = op;
            } else {
                allOps = key.interestOps() | op;
            }
            sc.register(_selector, allOps);
        } catch (ClosedChannelException e) {
            throw new MSNetSocketEndPipeException("Tried to register a socket that has been closed: "+  socket, e);
        } catch (CancelledKeyException e) {
            throw new MSNetSocketEndPipeException("Tried to register a socket channel that has been closed: "+  socket, e);
        } catch (Exception e) {
            throw new MSNetException("Error registering socket channel due to a closed socket or multiplexer: [socket="
                    + socket + " selector=" + _selector + "]", e);
        }
    }
    
    private static String describeSelectionKey(SelectionKey key) {
        String type = key.channel().toString();
        if (key.channel() instanceof SocketChannel) {
            type = ((SocketChannel) key.channel()).socket().getChannel().socket().toString();
        }
        String ops;
        try {
            int interestOps = key.interestOps();
            ops = MSNetChannelOpertaion.getOpsString(interestOps);
        } catch (CancelledKeyException e) {
            ops = "CANCELLED";
        }
        return type + " " + ops;
    }
    
    @SuppressWarnings("unused")
    private String describeKeySet(Set<SelectionKey> keys) {
        HashSet<String> descs = new HashSet<String>();
        for (SelectionKey key : keys) {
            descs.add(describeSelectionKey(key));
        }
        return descs.toString();
    }
    
    public MSNetMultiplexStatus multiplex(long timeout) {
        _status.clear();
        if (_selector == null) {
            _status.setException(new MSNetException("Multiplexer closed"));
            return _status;
        }
        if (gotWokenUp()) {
            handleWakeup();
            return _status;
        }
        try {
            int ret = 0;
            if (timeout < 0) {
                ret = _selector.select();
            } else if (timeout == 0) {
                ret = _selector.selectNow();
            } else {
                ret = _selector.select(timeout);
            }
            _status.setReturnCode(ret);
            
            handleJVMBug(ret);
        } catch (IOException x) {
            if ("Interrupted system call".equals(x.getMessage())) {
                _status.setException(new MSNetSelectInterruptException(x.getMessage()));
            } else {
                _status.setException(new MSNetException(x));
            }
            LOGGER.warn("Selector exception:" + x, x);
        } catch (Exception x) {
            LOGGER.warn("Selector exception:" + x, x);
            _status.setException(new MSNetException(x));
        }
        if (gotWokenUp()) {
            handleWakeup();
        }
        return _status;
    }
    public void wakeup() {
        _wokenUp = true;
        try {
            _selector.wakeup();
        } catch (NullPointerException e) {
            
            LOGGER.debug("Tried to call wakeup() on a closed multiplexer");
        }
        ;
    }
    public void setWakeupCallback(MSNetMultiplexer.WakeupListener listener) {
        _wakeupListener = listener;
    }
    public Selector getSelector() {
        return _selector;
    }
    public boolean isEventPending(MSNetTCPSocket socket, int op) {
        try {
            SelectableChannel sc = socket.getSelectableChannel();
            SelectionKey key = sc.keyFor(_selector);
            if (key == null) {
                return false;
            }
            return ((_selector.selectedKeys().contains(key)) && ((key.readyOps() & op) != 0));
        } catch (java.nio.channels.CancelledKeyException e) {
            return false;
        } catch (Throwable e) {
            
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(
                        "Event pending check failed due to a closed socket or multiplexer: [socket={} selector={}]",
                        socket, _selector);
            }
            return false;
        }
    }
    public void clearPendingEvent(MSNetTCPSocket socket, int type) {
        try {
            SelectableChannel sc = socket.getSelectableChannel();
            SelectionKey key = sc.keyFor(_selector);
            if (key == null) {
                return;
            }
            if (type == MSNetChannelOpertaion.ACCEPT.selectionKey) {
                _selector.selectedKeys().remove(key);
            } else {
                
                
                
                if (clearPendingEvent(key, type)) {
                    _selector.selectedKeys().remove(key);
                    key.cancel();
                }
            }
        } catch (Throwable e) {
            
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(
                        "Clear pending event failed due to a closed socket or multiplexer: [socket={} selector={}]",
                        socket, _selector);
            }
        }
    }
    public void clearAllPendingEvents() {
        SelectionKey key;
        try {
            for (Iterator<SelectionKey> i = _selector.selectedKeys().iterator(); i.hasNext();) {
                key = i.next();
                
                
                
                if ((key.isValid() == false) || clearPendingEvent(key, key.interestOps())) {
                    i.remove();
                }
            }
        } catch (Exception e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Tried to clear all pending events on an already closed multiplexer: [selector={}]",
                        _selector);
            }
        }
    }
    
    final boolean clearPendingEvent(SelectionKey key, int op_) {
        
        
        
        key.interestOps(key.interestOps() ^ op_);
        return (key.interestOps() == 0);
    }
    
    final void handleWakeup() {
        acknowledgeWakeup();
        if (_wakeupListener != null) {
            _wakeupListener.wakeupCallback();
        }
    }
    
    final void acknowledgeWakeup() {
        _wokenUp = false;
    }
    
    final boolean gotWokenUp() {
        return _wokenUp;
    }
    
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (_selector != null && _selector.isOpen()) {
            LOGGER.error("Multiplexer was not closed, this would have resulted in a fd leak");
            close();
        }
    }
    
    void handleJVMBug(int ret) throws IOException, ClosedChannelException, MSNetException {
        if (!BROKEN_NIO) {
            return;
        }
        if (ret != 0 || !_selector.selectedKeys().isEmpty()) {
            
            nioBugSpinTracker = 0;
            return;
        }
        if (++nioBugSpinTracker <= NIO_SPIN_MAX_COUNT) {
            
            return;
        }
        
        switch (NIO_SPIN_RECOVERY) {
        case CLOSE:
            if (nioBugSpinTracker > (1.5 * NIO_SPIN_MAX_COUNT)) {
                logForJVMBug("Detected possible 1.6.x JVM NIO Spinning Bug. Reparent selector to recover", true);
                synchronized (this) {
                    
                    final Selector new_selector = Selector.open();
                    for (SelectionKey k : _selector.keys()) {
                        if (k.isValid()) {
                            final SelectableChannel channel = k.channel();
                            final Object attachment = k.attachment();
                            if (attachment == null) {
                                channel.register(new_selector, k.interestOps());
                            } else {
                                register(new_selector, (MSNetChannel) attachment);
                            }
                            k.cancel();
                        }
                    }
                    _selector.close();
                    _selector = new_selector;
                }
                nioBugSpinTracker = 0;
                ++nioBugOccuranceCount;
                logForJVMBug("Closed & reparented selectors successfully for 1.6.x JVM NIO Spinning Bug.", true);
            } else if (nioBugSpinTracker == NIO_SPIN_MAX_COUNT + 1) {
                
                logForJVMBug("Detected possible 1.6.x JVM NIO Spinning Bug. Cancel keys with 0 interests", true);
                
                for (SelectionKey key : _selector.keys()) {
                    if (key.isValid() && key.interestOps() == 0) {
                        key.cancel();
                    }
                }
                logForJVMBug("Cancelled keys to recover from 1.6.x JVM NIO Spinning Bug.", true);
            }
            break;
        case QUIT:
            LOGGER.error("Exiting JVM due to 1.6.x JVM NIO Spinning Bug.");
            System.exit(-1);
            break;
        case WARN:
            logForJVMBug("Detected problem: " + NIO_BUG_MESSAGE, false);
            nioBugSpinTracker = 0;
            ++nioBugOccuranceCount;
            break;
        }
    }
    private void logForJVMBug(String message, boolean isDetails) {
        Logger chosenlogger = isDetails ? BUG_DETAILS_LOGGER : LOGGER;
        if (nioBugOccuranceCount < NIO_BUG_LOGGING_THRESHOLD || nioBugOccuranceCount % nioBugLoggingModControl == 0) {
            String countString = "(Count:" + nioBugOccuranceCount + ") ";
            if (isDetails) {
                chosenlogger.info(countString + message);
            } else {
                chosenlogger.error(countString + message);
            }
            if (nioBugOccuranceCount >= NIO_BUG_LOGGING_THRESHOLD) {
                nioBugLoggingModControl = nioBugLoggingModControl * 2;
            }
        } else {
            if (chosenlogger.isDebugEnabled()) {
                chosenlogger.debug("(Count:" + nioBugOccuranceCount + ") " + message);
            }
        }
    }
}
