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

package msjava.logbackutils.async;
import java.util.Iterator;
import msjava.logbackutils.internal.RuntimeShutdownHook;
import ch.qos.logback.access.spi.AccessEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.spi.AppenderAttachable;
import ch.qos.logback.core.spi.AppenderAttachableImpl;
public class AsyncAppender<E> extends UnsynchronizedAppenderBase<E> implements AppenderAttachable<E>, java.io.Flushable  {
    
    static {
        boolean isLoggingEvent;
        boolean isAccessEvent;
        try {
            Class.forName("ch.qos.logback.classic.spi.LoggingEvent");
            isLoggingEvent = true;
        } catch (ClassNotFoundException cnfe) {
            isLoggingEvent = false;
        }
        try {
            Class.forName("ch.qos.logback.access.spi.AccessEvent");
            isAccessEvent = true;
        } catch (ClassNotFoundException cnfe) {
            isAccessEvent = false;
        }
        isLoggingEventOnTheClassPath = isLoggingEvent;
        isAccessEventOnTheClassPath = isAccessEvent;
    }
    public static final boolean isLoggingEventOnTheClassPath;
    public static final boolean isAccessEventOnTheClassPath;
    
    private final AppenderAttachableImpl<E> aai = new AppenderAttachableImpl<E>();
    
    private int appenderCount = 0;
    
    private AsyncDispatcher<E> dispatcher;
    
    private volatile boolean firstAppend = true;
    private boolean callerDataInfo = false;
    private final RuntimeShutdownHook shutdownHook = new RuntimeShutdownHook(this::flushMessagesOnJvmExit);
    
    public void setCallerDataInfo(boolean callerDataInfo) {
        this.callerDataInfo = callerDataInfo;
        addInfo("CallerDataInfo set to :" + callerDataInfo);
    }
    
    public boolean getCallerDataInfo() {
        return callerDataInfo;
    }
    
    @Override
    protected void append(E eventObject) {
        appendLoopOnAppenders(eventObject);
    }
    
    private void appenderListHasChanged() {
        if (dispatcher != null) {
            dispatcher.setAppenders(iteratorForAppenders());
        } else {
            addError("Dispatcher has not been set!");
        }
    }
    
    public void addAppender(Appender<E> newAppender) {
        throwIfStarted();
        if (!aai.isAttached(newAppender)) {
            aai.addAppender(newAppender);
            ++appenderCount;
        }
    }
    
    @Override
    public synchronized void start() {
        appenderListHasChanged();
        super.start();
    }
    
    @Override
    public synchronized void stop() {
        try {
            super.stop();
            detachAndStopAllAppenders();
        } catch (Throwable t) {
            addError("Exception happens when stopping.", t);
        }
        
        try {
            shutdownHook.unregister();
        } catch (Throwable e) {
            addError("Got exception when stops.", e);
        }
    }
    
    private void prepareEventForDeferredProcessing(E e) {
        if (isLoggingEventOnTheClassPath) {
            if (e instanceof LoggingEvent) {
                LoggingEvent event = (LoggingEvent) e;
                event.prepareForDeferredProcessing();
                if (callerDataInfo) {
                    event.getCallerData();
                }
                event.getFormattedMessage();
            }
        }
        if (isAccessEventOnTheClassPath) {
            if (e instanceof AccessEvent) {
                ((AccessEvent) e).prepareForDeferredProcessing();
            }
        }
        
        
    }
    private void initializeIfNecessary() {
        if (firstAppend) {
            synchronized (this) {
                if (firstAppend) {
                    
                    try {
                        shutdownHook.register();
                    } catch (Throwable e) {
                        addError("Unable to register shutdown hook", e);
                    }
                    
                    startDispatcher();
                    
                    firstAppend = false;
                }
            }
        }
    }
    
    private void startDispatcher() {
        if (dispatcher != null) {
            dispatcher.start();
        } else {
            addError("Dispatcher has not been set.");
        }
    }
    
    public int appendLoopOnAppenders(E e) {
        initializeIfNecessary();
        prepareEventForDeferredProcessing(e);
        dispatcher.add(e);
        return appenderCount;
    }
    
    public Iterator<Appender<E>> iteratorForAppenders() {
        return aai.iteratorForAppenders();
    }
    
    public Appender<E> getAppender(String name) {
        return aai.getAppender(name);
    }
    
    public boolean isAttached(Appender<E> appender) {
        return aai.isAttached(appender);
    }
    
    public void detachAndStopAllAppenders() {
        
        if (dispatcher != null) {
            dispatcher.stop();
        }
        
        
        
        
        firstAppend = true;
    }
    
    public boolean detachAppender(Appender<E> appender) {
        throwIfStarted();
        boolean result = aai.detachAppender(appender);
        if (result) {
            --appenderCount;
            appenderListHasChanged();
        }
        return result;
    }
    
    public boolean detachAppender(String name) {
        throwIfStarted();
        boolean success = aai.detachAppender(name);
        if (success) {
            --appenderCount;
            appenderListHasChanged();
        }
        return success;
    }
    public void flush() {
        dispatcher.flush();
    }
    
    public AsyncDispatcher<E> getDispatcher() {
        return dispatcher;
    }
    
    public void setDispatcher(AsyncDispatcher<E> dispatcher) {
        throwIfStarted();
        this.dispatcher = dispatcher;
        if (dispatcher.getContext() == null) {
            dispatcher.setContext(getContext());
        }
    }
    private void throwIfStarted() {
        if (this.started) {
            throw new IllegalStateException(
                    "Can not add, remove, or detach an appender when the Async appender already started");
        }
    }
    private void flushMessagesOnJvmExit() {
        if (isStarted()) {
            addInfo("Please wait for AsyncAppender to stop and flush all the logging messages.");
            stop();
            addInfo("AsyncAppender successfully stopped and flushed all logging messages");
        }
    }
   
}
