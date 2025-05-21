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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.MapMaker;
import com.sun.management.OperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import static com.google.common.base.Preconditions.checkNotNull;
public class MSThreadMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MSThreadMonitor.class);
    
    public enum OutputMode {
        OFF, LOG, STD_OUT,
    }
    
    
    public interface ExtraInfoProvider {
        
        String getExtraInformation(boolean extended);
    }
    
    private static final String DEFAULT_TIMEOUT_PARAM = "30000";
    
    public static final long DEFAULT_TIMEOUT = setupTimeout();
    private static long setupTimeout() {
        String timeout = System.getProperty("msjava.msthreadmonitor.timeout", DEFAULT_TIMEOUT_PARAM);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Setting default timeout=" + timeout);
        }
        return Long.valueOf(timeout);
    }
    
    private static final String DEFAULT_FULL_DUMP_RATE_PARAM = "3";
    
    public static final int DEFAULT_FULL_DUMP_RATE = setupDefaultFullDumpRate();
    private static int setupDefaultFullDumpRate() {
        String fulldumprate = System.getProperty("msjava.msthreadmonitor.fulldumprate", DEFAULT_FULL_DUMP_RATE_PARAM);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Setting default fulldumprate=" + fulldumprate);
        }
        return Integer.valueOf(fulldumprate);
    }
    
    static final int DEFAULT_CONCUR_LEVEL = Math.min(4, Runtime.getRuntime().availableProcessors());
    
    private final ConcurrentMap<Thread, HeartbeatInfo> heartbeatByThread =
            new MapMaker().concurrencyLevel(DEFAULT_CONCUR_LEVEL).weakKeys().makeMap();
    
    private final List<MSThreadMonitorListener> listeners = new CopyOnWriteArrayList<>();
    
    private volatile OutputMode outputMode = OutputMode.LOG;
    
    private volatile long timeout = DEFAULT_TIMEOUT;
    
    private volatile int fullDumpRate = DEFAULT_FULL_DUMP_RATE;
    
    private final Lock lock = new ReentrantLock();
    
    private final ScheduledExecutorService execService =
            Executors.newScheduledThreadPool(DEFAULT_CONCUR_LEVEL, newThreadFactory("MonitorServiceThread"));
    
    private volatile ScheduledFuture<?> nextHeartbeatVerification;
    
    private boolean isUsingAsync = Boolean.parseBoolean(System.getProperty("msjava.msthreadmonitor.ThreadDumper.isUsingAsync"));
    
    private boolean isNativeMode = Boolean.parseBoolean(System.getProperty("msjava.msthreadmonitor.ThreadDumper.isNativeMode"));
    
    private ThreadDumper threadDumper = ThreadDumper.init(isUsingAsync, isNativeMode);
    
    private static final class Holder {
        static final MSThreadMonitor instance = new MSThreadMonitor();
    }
    
    public static MSThreadMonitor instance() {
        return Holder.instance;
    }
    
    protected MSThreadMonitor() {
        
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            
            ObjectName mxbeanName = new ObjectName(ThreadDumperSetterMXBean.OBJECTNAME);
            ThreadDumperSetter threadDumperSetter = this.new ThreadDumperSetter();
            
            mbs.registerMBean(threadDumperSetter, mxbeanName);
        } catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
            LOGGER.error("MXBean issue. ", e);
        }
    }
    
    void destroy(){
        try {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(new ObjectName(ThreadDumperSetterMXBean.OBJECTNAME));
        } catch (MBeanRegistrationException | InstanceNotFoundException | MalformedObjectNameException e) {
            LOGGER.error("Could not unregister " + ThreadDumperSetterMXBean.OBJECTNAME, e);
        }
    }
    
    private static ThreadFactory newThreadFactory(final String threadName) {
        return new ThreadFactory() {
            @Override
            public Thread newThread(Runnable task) {
                Thread thread = new Thread(task, threadName);
                thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        LOGGER.error("Uncaught exception in thread " + threadName + ": " + e.getMessage(), e);
                    }
                });
                thread.setDaemon(true);
                return thread;
            }
        };
    }
    public long getTimeout() {
        return timeout;
    }
    
    @VisibleForTesting
    void setTimeout(long timeout) {
        this.timeout = timeout;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Set timeout={}", timeout);
        }
        
        
        
        if (nextHeartbeatVerification != null
                && timeout < nextHeartbeatVerification.getDelay(TimeUnit.MILLISECONDS)
                && nextHeartbeatVerification.cancel(false)) {
            scheduleHeartbeatMonitor();
        }
    }
    
    public void setFullDumpRate(int fullDumpRate) {
        this.fullDumpRate = fullDumpRate;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Set fullDumpRate={}", fullDumpRate);
        }
    }
    public int getFullDumpRate() {
        return fullDumpRate;
    }
    public void setOutputMode(OutputMode outputMode) {
        this.outputMode = checkNotNull(outputMode);
    }
    
    public void setUsingAsync(boolean isUsingAsync) {
        LOGGER.info("MSThreadMonitor has fields isUsingAsync:{} and isNativeMode:{}.", isUsingAsync, isNativeMode);
        if (this.isUsingAsync == isUsingAsync)
            return;
        this.isUsingAsync = isUsingAsync;
        threadDumper = ThreadDumper.init(isUsingAsync, isNativeMode);
    }
    
    public void setNativeMode(boolean isNativeMode) {
        LOGGER.info("MSThreadMonitor has fields isUsingAsync:{} and isNativeMode:{}.", isUsingAsync, isNativeMode);
        if (this.isNativeMode == isNativeMode)
            return;
        this.isNativeMode = isNativeMode;
        
        if (threadDumper instanceof AsyncThreadDumper)
            ((AsyncThreadDumper) threadDumper).setNativeMode(isNativeMode);
    }
    public boolean isUsingAsync() {
        return isUsingAsync;
    }
    public boolean isNativeMode() {
        return isNativeMode;
    }
    public ThreadDumper getThreadDumper() {
        return threadDumper;
    }
    public OutputMode getOutputMode() {
        return outputMode;
    }
    public void addListener(MSThreadMonitorListener listener) {
        listeners.add(checkNotNull(listener));
    }
    public void removeListener(MSThreadMonitorListener listener) {
        listeners.remove(checkNotNull(listener));
    }
    
    public Long register() {
        return register(DEFAULT_TIMEOUT, null);
    }
    
    public Long register(@Nullable ExtraInfoProvider extraInfoProvider) {
        return register(DEFAULT_TIMEOUT, extraInfoProvider);
    }
    
    public Long register(long timeout) {
        return register(timeout, null);
    }
    
    public Long register(long timeout, @Nullable ExtraInfoProvider extraInfoProvider) {
        lock.lock();
        try {
            int tid = AsyncThreadDumper.getTidIfLoaded();
            long prevTimeout = 0;
            Thread thread = Thread.currentThread();
            long timeNow = System.currentTimeMillis();
            HeartbeatInfo heartbeat = heartbeatByThread.get(thread);
            if (heartbeat == null) {
                heartbeatByThread.put(thread, new HeartbeatInfo(timeNow, timeout, extraInfoProvider, tid));
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Thread {} with id {} registered - timeout={}", thread, thread.getId(), timeout);
                }
            } else {
                heartbeat.lastHeartbeat = timeNow;
                heartbeat.extraInfoProvider = extraInfoProvider;
                prevTimeout = heartbeat.timeout;
                if (prevTimeout == timeout) {
                    LOGGER.warn("Thread {} with id {} has already been registered - timeout={}", thread, thread.getId(), timeout);
                } else {
                    heartbeat.timeout = timeout;
                    LOGGER.warn("Thread {} with id {} has already been registered - updated timeout={}", thread, thread.getId(), timeout);
                }
            }
            
            if (heartbeatByThread.size() == 1 || timeout < this.timeout) {
                
                setTimeout(timeout);
            } else if (prevTimeout > 0 && prevTimeout == this.timeout && prevTimeout < timeout) {
                
                setTimeout(computeTimeout(prevTimeout));
            }
            if (!isMonitoring()) {
                startMonitoring();
            }
            return prevTimeout > 0 ? prevTimeout : null;
        } finally {
            lock.unlock();
        }
    }
    
    public void unregister() {
        lock.lock();
        try {
            Thread thread = Thread.currentThread();
            HeartbeatInfo heartbeat = heartbeatByThread.remove(thread);
            if (heartbeat == null) {
                throw new IllegalStateException(String.format("Thread %s with id %d has not been registered with the monitor", thread, thread.getId()));
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Thread {} with id {} unregistered", thread, thread.getId());
            }
            if (heartbeatByThread.isEmpty()) {
                stopMonitoring();
                setTimeout(DEFAULT_TIMEOUT);
            } else if (heartbeat.timeout == this.timeout) {
                
                setTimeout(computeTimeout(this.timeout));
            }
        } finally {
            lock.unlock();
        }
    }
    
    
    public boolean isRegistered() {
        return isRegistered(Thread.currentThread());
    }
    
    public boolean isRegistered(Thread thread) {
        lock.lock();
        try {
            return heartbeatByThread.containsKey(thread);
        } finally {
            lock.unlock();
        }
    }
    
    public boolean unregisterIfRegistered() {
        lock.lock();
        try {
            if (isRegistered()) {
                unregister();
                return true;
            } else {
                return false;
            }
        } finally {
            lock.unlock();
        }
    }
    
    private long computeTimeout(long currTimeout) {
        long newTimeout = Long.MAX_VALUE;
        for (HeartbeatInfo heartbeat : heartbeatByThread.values()) {
            if (heartbeat.timeout < newTimeout) {
                newTimeout = heartbeat.timeout;
                if (newTimeout == currTimeout) {
                    break;
                }
            }
        }
        return newTimeout;
    }
    
    public void heartbeat() {
        final Thread thread = Thread.currentThread();
        HeartbeatInfo heartbeat = heartbeatByThread.get(thread);
        if (heartbeat == null) {
            throw new IllegalStateException(String.format("Thread %s with id %d has not been registered with the monitor", thread, thread.getId()));
        }
        long timeNow = System.currentTimeMillis();
        
        
        final long delay = timeNow - heartbeat.lastHeartbeat;
        if (delay > heartbeat.timeout) {
            selfThreadAlert(thread, delay);
            if (!listeners.isEmpty()) {
                notifyListeners(thread, delay);
            }
        }
        heartbeat.dumpCnt = 0;
        heartbeat.lastHeartbeat = timeNow;
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Thread {} with id {} heartbeats @ timeNow={}...", thread, thread.getId(), timeNow);
        }
    }
    
    private void notifyListeners(final Thread thread, final long delay) {
        execService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                for (MSThreadMonitorListener listener : listeners) {
                    listener.heartbeatStallingCallback(thread, delay);
                }
                return null;
            }
        });
    }
    private void startMonitoring() {
        if (nextHeartbeatVerification == null
                || nextHeartbeatVerification.isCancelled()) {
            scheduleHeartbeatMonitor();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Start monitoring...");
            }
        }
    }
    private void stopMonitoring() {
        if (nextHeartbeatVerification != null) {
            nextHeartbeatVerification.cancel(false);
            nextHeartbeatVerification = null;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Stop monitoring...");
            }
        }
    }
    
    @VisibleForTesting
    boolean isMonitoring() {
        return nextHeartbeatVerification != null && !nextHeartbeatVerification.isCancelled();
    }
    
    private void scheduleHeartbeatMonitor() {
        nextHeartbeatVerification = execService.schedule(heartbeatVerifier, this.timeout, TimeUnit.MILLISECONDS);
    }
    
    private void checkHeartbeats() {
        for (Map.Entry<Thread, HeartbeatInfo> entry : heartbeatByThread.entrySet()) {
            final Thread thread = entry.getKey();
            if (thread != null) {
                HeartbeatInfo heartbeat = entry.getValue();
                long delay = System.currentTimeMillis() - heartbeat.lastHeartbeat;
                if (delay > heartbeat.timeout) {
                    if (shouldDoFullDump(heartbeat)) {
                        allThreadAlert(thread, delay, heartbeat.getExtraInfo(true));
                    } else {
                        monitoredThreadAlert(thread, delay, heartbeat.getExtraInfo(false));
                    }
                    if (!listeners.isEmpty()) {
                        notifyListeners(thread, delay);
                    }
                }
            }
        }
    }
    
    private void selfThreadAlert(Thread thread, long delay) {
        switch (outputMode) {
            case LOG:
                LOGGER.warn("Thread {} with id {} has been stalled - no heartbeat received in {} ms", thread, thread.getId(), delay);
                break;
            case STD_OUT:
                System.out.printf("Thread %s with id %d has been stalled - no heartbeat received in %d ms", thread, thread.getId(), delay);
                break;
            case OFF:
            default:
        }
    }
    
    private void monitoredThreadAlert(Thread thread, long delay, String extraInfo) {
        HeartbeatInfo heartbeat = heartbeatByThread.get(thread);
        if (heartbeat == null) {
            
            allThreadAlert(thread, delay, extraInfo);
        } else {
            int javaSysTid = heartbeat.tid;
            if (threadDumper instanceof AsyncThreadDumper && javaSysTid == AsyncProfiler.INVALID_THREAD_ID) {
                
                allThreadAlert(thread, delay, extraInfo);
            } else {
                String stackTrace = threadDumper.getStackTrace(thread, javaSysTid);
                switch (outputMode) {
                    case LOG:
                        LOGGER.warn(
                                "Thread {} with id {} is being stalled - no heartbeat received in {} ms:\nJVM stats:\n{}\nExtra information:\n{}\nthread dump:\n{}",
                                thread, thread.getId(), delay, JVMStats.getJvmStats(), extraInfo, stackTrace);
                        break;
                    case STD_OUT:
                        System.out.printf(
                                "Thread %s with id %d is being stalled - no heartbeat received in %d ms\nJVM stats:\n%s\nExtra information:\n%s\nthread dump:\n",
                                thread, thread.getId(), delay, JVMStats.getJvmStats(), extraInfo);
                        System.out.println(stackTrace);
                        break;
                    case OFF:
                    default:
                }
            }
        }
    }
    
    private void allThreadAlert(Thread thread, long delay, String extraInfo) {
        switch (outputMode) {
            case LOG:
                LOGGER.warn(
                    "Thread {} with id {} is being stalled - no heartbeat received in {} ms:\nJVM stats:\n{}\nExtra information:\n{}\nall thread dump:\n{}",
                    thread, thread.getId(), delay, JVMStats.getJvmStats(), extraInfo,
                        threadDumper.getAllStackTraces());
                break;
            case STD_OUT:
                System.out.printf("Thread %s with id %d is being stalled - no heartbeat received in %d ms:\nJVM stats:\n%s\nExtra information:\n%s\nall thread dump:\n",
                        thread, thread.getId(), delay, JVMStats.getJvmStats(), extraInfo);
                System.out.println(threadDumper.getAllStackTraces());;
                break;
            case OFF:
            default:
        }
    }
    
    private boolean shouldDoFullDump(HeartbeatInfo heartbeat) {
        return fullDumpRate > 0 && ++heartbeat.dumpCnt % fullDumpRate == 0;
    }
    
    private final Runnable heartbeatVerifier = new Runnable() {
        @Override
        public void run() {
            if (!execService.isShutdown() && !Thread.currentThread().isInterrupted()) {
                checkHeartbeats();
                scheduleHeartbeatMonitor();
            }
        }
    };
    
    private static final class HeartbeatInfo {
        volatile ExtraInfoProvider extraInfoProvider;
        volatile long lastHeartbeat;
        volatile long timeout;
        volatile int dumpCnt;
        final int tid;
        HeartbeatInfo(long lastHeartbeat, long timeout, ExtraInfoProvider extraInfoProvider, int tid) {
            this.lastHeartbeat = lastHeartbeat;
            this.timeout = timeout;
            this.extraInfoProvider = extraInfoProvider;
            this.tid = tid;
        }
        
        String getExtraInfo(boolean extended) {
            ExtraInfoProvider extraInfoProvider = this.extraInfoProvider;
            if (extraInfoProvider != null) {
                return extraInfoProvider.getExtraInformation(extended);
            } else {
                return "";
            }
        }
    }
    public interface ThreadDumperSetterMXBean {
        String OBJECTNAME = "msjava.threadmonitor.thread:type=ThreadDumperSetter";
        void setUsingAsync(boolean isUsingAsync);
        void setNativeMode(boolean isNativeMode);
        boolean isUsingAsync();
        boolean isNativeMode();
    }
    public class ThreadDumperSetter implements ThreadDumperSetterMXBean {
        @Override
        public void setUsingAsync(boolean isUsingAsync) {
            MSThreadMonitor.this.setUsingAsync(isUsingAsync);
        }
        @Override
        public void setNativeMode(boolean isNativeMode) {
            MSThreadMonitor.this.setNativeMode(isNativeMode);
        }
        @Override
        public boolean isUsingAsync() {
            return (MSThreadMonitor.this.threadDumper instanceof AsyncThreadDumper);
        }
        @Override
        public boolean isNativeMode() {
            return MSThreadMonitor.this.isNativeMode;
        }
    }
    
    @SuppressWarnings("restriction")
    private static final class JVMStats {
        private static final OperatingSystemMXBean osMXBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        private static final List<GarbageCollectorMXBean> gcMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
        private static final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        
        public static long getCommittedVirtualMemorySize() {
            return osMXBean.getCommittedVirtualMemorySize();
        }
        
        public static long getFreePhysicalMemorySize() {
            return osMXBean.getFreePhysicalMemorySize();
        }
        
        public static long getFreeSwapSpaceSize() {
            return osMXBean.getFreeSwapSpaceSize();
        }
        
        public static double getProcessCpuLoad() {
            return osMXBean.getProcessCpuLoad();
        }
        
        public static long getProcessCpuTime() {
            return osMXBean.getProcessCpuTime();
        }
        
        public static double getSystemCpuLoad() {
            return osMXBean.getSystemCpuLoad();
        }
        
        public static long getTotalPhysicalMemorySize() {
            return osMXBean.getTotalPhysicalMemorySize();
        }
        
        public static double getTotalSwapSpaceSize() {
            return osMXBean.getTotalSwapSpaceSize();
        }
        
        public static double getSystemLoadAverage() {
            return osMXBean.getSystemLoadAverage();
        }
        
        public static long getGarbageCollectionTime() {
            long collectionTime = 0;
            for (GarbageCollectorMXBean gcMXBean : gcMXBeans) {
                collectionTime += gcMXBean.getCollectionTime();
            }
            return collectionTime;
        }
        
        public static long getTotalStartedThreadCount() {
            return threadMXBean.getTotalStartedThreadCount();
        }
        
        public static long getPeakThreadCount() {
            return threadMXBean.getPeakThreadCount();
        }
        
        public static long getCurrentThreadCount() {
            return threadMXBean.getThreadCount();
        }
        
        public static long getDaemonThreadCount() {
            return threadMXBean.getDaemonThreadCount();
        }
        
        public static String getJvmStats() {
            return "TotalPhysicalMemorySize=" + getTotalPhysicalMemorySize() + ", " +
                    "CommittedVirtualMemorySize=" + getCommittedVirtualMemorySize() + ", " +
                    "FreePhysicalMemorySize=" + getFreePhysicalMemorySize() + ", " +
                    "TotalSwapSpaceSize=" + getTotalSwapSpaceSize() + ", " +
                    "FreeSwapSpaceSize=" + getFreeSwapSpaceSize() + ", " +
                    "ProcessCpuTime=" + getProcessCpuTime() + ", " +
                    "ProcessCpuLoad=" + getProcessCpuLoad() + ", " +
                    "SystemCpuLoad=" + getSystemCpuLoad() + ", " +
                    "SystemLoadAverage=" + getSystemLoadAverage() + ", " +
                    "GarbageCollectionTime=" + getGarbageCollectionTime() + ", " +
                    "TotalStartedThreadCount=" + getTotalStartedThreadCount() + ", " +
                    "PeakThreadCount=" + getPeakThreadCount() + ", " +
                    "CurrentThreadCount=" + getCurrentThreadCount() + ", " +
                    "DaemonThreadCount=" + getDaemonThreadCount();
        }
    }
}