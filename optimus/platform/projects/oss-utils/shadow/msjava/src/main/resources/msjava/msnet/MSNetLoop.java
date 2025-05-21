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
import java.util.concurrent.CopyOnWriteArrayList;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import com.google.common.base.Preconditions;
import msjava.base.slf4j.ContextLogger;
import msjava.msnet.utils.MSNetConfiguration;
public class MSNetLoop {
    
    private static final Logger LOGGER = ContextLogger.safeLogger();
    public static final MSNetLoopNullImpl NULL_IMPL = new MSNetLoopNullImpl();
    
    public static interface MSNetLoopErrorHandler {
        
        boolean handle(Throwable t, MSNetLoop loop) throws Throwable;
    }
    private static final String ERROR_HANDLING_MESSAGE = "This is only a failsafe, please don't throw from MSNet callbacks. "
            + "You can change this loop error handling strategy per netloop "
            + "using msjava.msnet.MSNetLoop.setLoopErrorHandler(MSNetLoopErrorHandler) or you can set a global "
            + "default using msjava.msnet.utils.MSNetConfiguration.setDefaultLoopErrorHandler(MSNetLoopErrorHandler), or "
            + "by specifying the msjava.msnet.default_loop_error_handling_strategy system property. See list of predefined "
            + "strategies in msjava.msnet.MSNetLoop.MSNetLoopErrorHandlingStrategy.";
    
    public static enum MSNetLoopErrorHandlingStrategy implements MSNetLoopErrorHandler {
        
        LOG_AND_RETHROW {
            @Override
            public boolean handle(Throwable t, MSNetLoop loop) throws Throwable {
                LOGGER.error(
                        "Uncaught exception in loop " + loop + ", rethrowing per strategy.\n" + ERROR_HANDLING_MESSAGE,
                        t);
                throw t;
            }
        },
        
        LOG_AND_QUIT {
            @Override
            public boolean handle(Throwable t, MSNetLoop loop) throws Throwable {
                LOGGER.error("Uncaught exception in loop " + loop + ", quitting loop per strategy.\n"
                        + ERROR_HANDLING_MESSAGE, t);
                return false;
            }
        },
        
        LOG_AND_CONTINUE {
            @Override
            public boolean handle(Throwable t, MSNetLoop loop) throws Throwable {
                LOGGER.warn("Uncaught exception in loop " + loop + ", continuing loop per strategy.\n"
                        + ERROR_HANDLING_MESSAGE, t);
                return true;
            }
        },
        
        LOG_AND_SYSTEM_EXIT {
            @Override
            public boolean handle(Throwable t, MSNetLoop loop) throws Throwable {
                LOGGER.error("Uncaught exception in loop " + loop + ", calling System.exit(1) per strategy.\n"
                        + ERROR_HANDLING_MESSAGE, t);
                try {
                    System.exit(1);
                } catch (Throwable tt) {
                    tt.addSuppressed(t);
                    throw tt;
                }
                throw t;
            }
        };
    }
    
    
    public static class MaxLoopRestartsErrorHandler implements MSNetLoopErrorHandler {
        private final int maxLoopRestarts;
        private final ThreadLocal<Integer> loopRestarts = new ThreadLocal<Integer>() {
            protected Integer initialValue() {
                return 0;
            };
        };
        
        public MaxLoopRestartsErrorHandler(int maxLoopRestarts) {
            Preconditions.checkArgument(maxLoopRestarts > 0, "maxLoopRestarts argument must be greated than 0");
            this.maxLoopRestarts = maxLoopRestarts;
        }
        @Override
        public boolean handle(Throwable t, MSNetLoop loop) throws Throwable {
            int loopRestarts = this.loopRestarts.get() + 1;
            this.loopRestarts.set(loopRestarts);
            if (loopRestarts > maxLoopRestarts) {
                LOGGER.error("Uncaught exception in netloop " + loop + ", max loop restarts of " + maxLoopRestarts
                        + " exceeded, quitting loop.", t);
                throw t;
            } else {
                LOGGER.warn("Uncaught exception in netloop " + loop + ", max loop restart " + loopRestarts + " of "
                        + maxLoopRestarts, t);
                return true;
            }
        }
    }
    
    
    public static interface MSNetLoopStatusListener {
        
        void loopStarting(MSNetLoop loop);
        
        void loopStopped(MSNetLoop loop);
        
        void loopStoppedWithError(MSNetLoop loop, Throwable t);
    }
    protected volatile MSNetLoopImpl impl;
    @Nonnull
    private volatile MSNetLoopErrorHandler loopErrorHandler = MSNetConfiguration.getDefaultLoopErrorHandler();;
    
    private final CopyOnWriteArrayList<MSNetLoopStatusListener> listeners = new CopyOnWriteArrayList<>();
    
    public MSNetLoop() throws MSNetLoopAlreadyExistsException {
        this(new MSNetLoopDefaultImpl());
    }
    
    public MSNetLoop(MSNetLoopImpl impl) {
        setImpl(impl);
    }
    
    public MSNetLoopErrorHandler getLoopErrorHandler() {
        MSNetLoopImpl impl = this.impl;
        if (impl != null) {
            return impl.getLoopErrorHandler();
        }
        return loopErrorHandler;
    }
    
    public void setLoopErrorHandler(MSNetLoopErrorHandler loopErrorHandler) {
        if (loopErrorHandler == null) {
            this.loopErrorHandler = MSNetConfiguration.getDefaultLoopErrorHandler();
        } else {
            this.loopErrorHandler = loopErrorHandler;
        }
        MSNetLoopImpl impl = this.impl;
        if (impl != null) {
            impl.setLoopErrorHandler(this.loopErrorHandler);
        }
    }
    
    public void addLoopStatusListener(MSNetLoopStatusListener listener) {
        if (listener != null) {
            listeners.add(listener);
        }
    }
    
    public void removeLoopStatusListener(MSNetLoopStatusListener listener) {
        if (listener != null) {
            listeners.remove(listener);
        }
    }
    
    public void setImpl(MSNetLoopImpl impl) {
        if (impl == null) {
            this.impl = NULL_IMPL;
        } else {
            this.impl = impl;
            if (impl.getLoopErrorHandler() == MSNetConfiguration.getDefaultLoopErrorHandler()) {
                impl.setLoopErrorHandler(loopErrorHandler);
            }
        }
    }
    public MSNetLoopImpl getImpl() {
        return impl;
    }
    
    public boolean getContinueLoop() {
        return impl.getContinueLoop();
    }
    
    public void setContinueLoop(boolean continueLoop) {
        impl.setContinueLoop(continueLoop);
    }
    
    public void wakeup() {
        impl.wakeup();
    }
    
    public void wakeup(final Runnable r) {
        MSNetLoopImpl i = impl;
        i.callbackAfterDelay(0, new MSNetEventListener() {
            public void eventOccurred(MSNetEvent e) {
                r.run();
            }
        });
        i.wakeup();
    }
    
    public boolean isLooping() {
        return impl.isLooping();
    }
    
    public void loop() throws MSNetWrongThreadException, MSNetAlreadyLoopingException {
        try {
            notifyLoopStarting();
            impl.loop();
        } catch (Throwable t) {
            notifyLoopStoppedWithError(t);
            throw t;
        }
        notifyLoopStopped();
    }
    private void notifyLoopStarting() {
        for (MSNetLoopStatusListener listener : listeners) {
            try {
                listener.loopStarting(this);
            } catch (Throwable t) {
                LOGGER.warn("MSNetLoopStatusListener " + listener + " threw, ignoring.", t);
            }
        }
    }
    private void notifyLoopStopped() {
        for (MSNetLoopStatusListener listener : listeners) {
            try {
                listener.loopStopped(this);
            } catch (Throwable t) {
                LOGGER.warn("MSNetLoopStatusListener " + listener + " threw, ignoring.", t);
            }
        }
    }
    private void notifyLoopStoppedWithError(Throwable t) {
        for (MSNetLoopStatusListener listener : listeners) {
            try {
                listener.loopStoppedWithError(this, t);
            } catch (Throwable tt) {
                LOGGER.warn("MSNetLoopStatusListener " + listener + " threw, ignoring.", tt);
            }
        }
    }
    
    public void addLoopListener(MSNetLoopListener listener) {
        impl.addLoopListener(listener);
    }
    
    public void removeLoopListener(MSNetLoopListener listener) {
        impl.removeLoopListener(listener);
    }
    
    public void registerChannel(MSNetChannel channel) {
        impl.registerChannel(channel);
    }
    
    public void unregisterChannel(MSNetChannel channel) {
        impl.unregisterChannel(channel);
    }
    
    public Object callbackAt(long timeToRunAt, MSNetEventListener listener) {
        return callbackAt(timeToRunAt, listener, null);
    }
    
    public Object callbackAt(long timeToRunAt, MSNetEventListener listener, String tag) {
        return impl.callbackAt(timeToRunAt, listener, tag);
    }
    
    public Object callbackAfterDelay(long delayInMillis, MSNetEventListener listener) {
        return callbackAfterDelay(delayInMillis, listener, null);
    }
    
    public Object callbackAfterDelay(long delayInMillis, MSNetEventListener listener, String tag) {
        return impl.callbackAfterDelay(delayInMillis, listener, tag);
    }
    
    public Object callbackPeriodically(long periodInMillis, MSNetEventListener listener) {
        return callbackPeriodically(periodInMillis, listener, null);
    }
    
    public Object callbackPeriodically(long periodInMillis, MSNetEventListener listener, String tag) {
        return impl.callbackPeriodically(periodInMillis, listener, tag);
    }
    
    public void setCallbackTag(Object id, String tag) {
        impl.setCallbackTag(id, tag);
    }
    
    public void cancelCallback(Object id) {
        impl.cancelCallback(id);
    }
    public void quit() {
        impl.quit();
    }
    
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null)
            return false;
        if (getClass() == o.getClass()) {
            MSNetLoopImpl i = ((MSNetLoop) o).impl;
            if ((i == impl) || 
                    ((impl != null) && (impl.equals(i)))) {
                return true;
            }
        }
        return false;
    }
    
    public int hashCode() {
        MSNetLoopImpl i = impl;
        if (i == null) {
            return 0;
        }
        return i.hashCode();
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[MSNetLoop on thread [");
        Thread t = getThreadForLoop(this);
        if (t == null) {
            sb.append("null");
        } else {
            sb.append(t.getName()).append(" (").append(t.getId()).append(")");
        }
        sb.append("], impl [");
        if (impl == null) {
            sb.append("null");
        } else {
            sb.append(impl.getClass().getName()).append("#0x")
                    .append(Integer.toHexString(System.identityHashCode(impl)));
        }
        sb.append("]]");
        return sb.toString();
    }
    
    public static MSNetLoop getLoopForThread(Thread thread) {
        MSNetLoopImpl impl = MSNetLoopThreadManager.getLoopImplForThread(thread);
        if (impl == null) {
            return null;
        }
        return new MSNetLoop(impl);
    }
    
    public static Thread getThreadForLoop(MSNetLoop loop) {
        return MSNetLoopThreadManager.getThreadForLoopImpl(loop.getImpl());
    }
    
    public static MSNetLoop getLoopingLoop(String name, boolean daemonize) {
        MSNetLoopThread loopThread = new MSNetLoopThread(name);
        loopThread.setDaemon(daemonize);
        loopThread.start();
        return loopThread.getLoop();
    }
}
