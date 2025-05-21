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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import msjava.base.lang.StackTraceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import msjava.msnet.MSNetLoop.MSNetLoopErrorHandler;
import msjava.msnet.MSNetLoop.MSNetLoopErrorHandlingStrategy;
import msjava.msnet.MSNetLoop.MSNetLoopStatusListener;
import msjava.msnet.MSNetLoop.MaxLoopRestartsErrorHandler;
import msjava.msnet.utils.MSNetConfiguration;
public class MSNetLoopThread extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(MSNetLoopThread.class);
    
    
    public static final String DEFAULT_LOOP_IMPL = "msjava.msnet.MSNetLoopDefaultImpl";
    
    static private AtomicLong id = new AtomicLong(0);
    protected volatile MSNetLoop loop;
    protected Class<?> implClass;
    
    protected final CountDownLatch startLatch = new CountDownLatch(1);;
    
    protected volatile Throwable startError;
    protected boolean restartLoopOnException;
    
    @Nonnull
    private volatile MSNetLoopErrorHandler loopErrorHandler = MSNetConfiguration.getDefaultLoopErrorHandler();;
    
    private final CopyOnWriteArrayList<MSNetLoopStatusListener> listeners = new CopyOnWriteArrayList<>();
    
    public MSNetLoopThread(String name) {
        this(name, false);
    }
    
    @Deprecated
    public MSNetLoopThread(String name, boolean restartLoopOnException) {
        this(name, DEFAULT_LOOP_IMPL, restartLoopOnException);
    }
    
    @Deprecated
    public MSNetLoopThread(String name_, String implClassName_, boolean restartLoopOnException) {
        super(name_ + " [MSNetLoopThread#" + nextId() + "]");
        this.restartLoopOnException = restartLoopOnException;
        setDaemon(true);
        try {
            implClass = Class.forName(implClassName_);
        } catch (ClassNotFoundException e) {
            
            throw new MSNetRuntimeException("Couldn't find loop impl class!");
        }
    }
    
    public MSNetLoopThread(String name, String implClassName) {
        this(name, implClassName, false);
    }
    
    public MSNetLoopErrorHandler getLoopErrorHandler() {
        MSNetLoop loop = this.loop;
        if (loop != null) {
            return loop.getLoopErrorHandler();
        }
        return loopErrorHandler;
    }
    
    public void setLoopErrorHandler(MSNetLoopErrorHandler loopErrorHandler) {
        if (loopErrorHandler == null) {
            this.loopErrorHandler = MSNetConfiguration.getDefaultLoopErrorHandler();
        } else {
            this.loopErrorHandler = loopErrorHandler;
        }
        MSNetLoop loop = this.loop;
        if (loop != null) {
            loop.setLoopErrorHandler(this.loopErrorHandler);
        }
    }
    
    public void addMSNetLoopStatusListener(MSNetLoopStatusListener listener) {
        if (listener != null) {
            listeners.add(listener);
        }
        MSNetLoop loop = this.loop;
        if (loop != null) {
            loop.addLoopStatusListener(listener);
        }
    }
    
    public void removeMSNetLoopStatusListener(MSNetLoopStatusListener listener) {
        if (listener != null) {
            listeners.remove(listener);
        }
        MSNetLoop loop = this.loop;
        if (loop != null) {
            loop.removeLoopStatusListener(listener);
        }
    }
    protected static long nextId() {
        return id.getAndIncrement();
    }
    
    public MSNetLoop startLoop() {
        start();
        return getLoop();
    }
    
    public void start() {
        super.start();
        try {
            if (!startLatch.await(MSNetConfiguration.getLoopStartupTimeout(), TimeUnit.MILLISECONDS)) {
                StackTraceUtils.logAllStackTraces(LOG);
                throw new MSNetRuntimeException("Couldn't start loop after "
                        + MSNetConfiguration.getLoopStartupTimeout() + " ms.");
            }
        } catch (InterruptedException x) {
            throw new MSNetRuntimeException("Interrupted starting loop.");
        }
        if (startError != null) {
            if (startError instanceof RuntimeException) {
                throw (RuntimeException) startError;
            } 
            if (startError instanceof Error) {
                throw (Error) startError;
            } 
            throw new MSNetRuntimeException("Error starting loop", startError);
        }
    }
    public void run() {
        MSNetLoopImpl impl = null;
        try {
            impl = (MSNetLoopImpl) implClass.newInstance();
            loop = new MSNetLoop(impl);
            loop.setLoopErrorHandler(loopErrorHandler);
            for (MSNetLoopStatusListener listener : listeners) {
                loop.addLoopStatusListener(listener);
            }
            loop.callbackAfterDelay(0, new MSNetEventListener() {
                public void eventOccurred(MSNetEvent e) {
                    startLatch.countDown();
                }
            });
        } catch (Throwable t) {
            
            startError = t;
            startLatch.countDown();
            return;
        }
        while (true) {
            try {
                loop.loop();
                
                return;
            } catch (Throwable t) {
                if (!handleError(t)) {
                    if (startLatch.getCount() > 1) {
                        
                        
                        startError = t;
                    }
                    startLatch.countDown();
                    
                    LOG.error("Uncaught exception in loop, MSNetLoop thread " + getName()+ " terminating.", t);
                    if (t instanceof Error) {
                        throw (Error) t;
                    }
                    if (t instanceof RuntimeException) {
                        throw (RuntimeException) t;
                    }
                    throw new MSNetRuntimeException("Uncaught exception in loop", t);
                }
                LOG.warn("Uncaught exception in loop", t);
            }
        }
    }
    
    @Deprecated
    protected boolean handleError(Throwable t) {
        return restartLoopOnException;
    }
    public MSNetLoop getLoop() {
        return loop;
    }
}
