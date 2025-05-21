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
package msjava.msnet.internal;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import java.util.EnumMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.CheckReturnValue;
import msjava.base.lang.StackTraceUtils;
import msjava.base.slf4j.ContextLogger;
import msjava.msnet.utils.MSNetConfiguration;
import org.slf4j.Logger;
public abstract class MSNetConnectingLock {
    private static final Logger LOG = ContextLogger.safeLogger();
    public enum State {
        
        NOT_LOCKED(true, false),
        
        LOCKED(false, false),
        
        LOCKED_SCHEDULED(false, false),
        
        SCHEDULED(false, true),
        
        LOCKED_ASYNC(false, false),
        
        LOCKED_FOR_RESET(false, false);
        final boolean canAcquire;
        final boolean canAsyncAcquire;
        State(boolean canAcquire, boolean canAsyncAcquire) {
            this.canAcquire = canAcquire;
            this.canAsyncAcquire = canAsyncAcquire;
        }
    }
    
    private static final EnumMap<State, Set<State>> VALID_TRANSITIONS = new EnumMap<>(State.class);
    static {
        VALID_TRANSITIONS.put(State.NOT_LOCKED, Sets.immutableEnumSet( 
            State.LOCKED, 
            State.LOCKED_FOR_RESET 
        ));
        VALID_TRANSITIONS.put(State.LOCKED, Sets.immutableEnumSet( 
            State.NOT_LOCKED, 
            State.LOCKED_SCHEDULED 
        ));
        VALID_TRANSITIONS.put(State.LOCKED_SCHEDULED, Sets.immutableEnumSet( 
            State.SCHEDULED, 
            State.NOT_LOCKED 
        ));
        VALID_TRANSITIONS.put(State.SCHEDULED, Sets.immutableEnumSet( 
            State.LOCKED_ASYNC, 
            State.LOCKED_FOR_RESET, 
            State.NOT_LOCKED 
        ));
        VALID_TRANSITIONS.put(State.LOCKED_ASYNC, Sets.immutableEnumSet( 
            State.LOCKED_SCHEDULED, 
            State.SCHEDULED, 
            State.NOT_LOCKED 
        ));
        VALID_TRANSITIONS.put(State.LOCKED_FOR_RESET, Sets.immutableEnumSet( 
            State.NOT_LOCKED 
        ));
    }
    private State state = State.NOT_LOCKED;
    
    private Thread lockOwnerThread = null;
    
    private long clearThreadId;
    
    private String clearThreadName;
    private int asyncEventId = 0;
    public static MSNetConnectingLock newInstance() {
        return MSNetConfiguration.CONNECTINGLOCK_OLD_IMPL ?
            new MSNetConnectingLockMonitor() :
            new MSNetConnectingLockLock();
    }
    abstract void signalAll();
    abstract void await() throws InterruptedException;
    abstract void await(long rem) throws InterruptedException;
    void setState(State s, Thread newLockOwnerThread, boolean clear) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("setState({}, {}, {}) called on {}\nStack trace:\n{}", s, newLockOwnerThread, clear, this,
                StackTraceUtils.currentStackTraceToString());
        }
        if (!VALID_TRANSITIONS.get(state).contains(s)) {
            throw new IllegalStateException("Invalid state change " + state + "->" + s + " on " + this);
        }
        if (newLockOwnerThread == null && !(s.canAcquire || s.canAsyncAcquire)) { 
            throw new IllegalStateException(
                this + ": newLockOwnerThread is null, but new state (" + s + ") doesn't allow to acquire lock");
        }
        if (newLockOwnerThread != null && (s.canAcquire || s.canAsyncAcquire)) { 
            throw new IllegalStateException(this + ": newLockOwnerThread is not null (" + newLockOwnerThread
                + "), but new state (" + s + ") still allows to acquire lock");
        }
        if (clear && newLockOwnerThread != null) {
            throw new IllegalStateException(
                "Clear signalled but new lock owner thread is not null, but " + newLockOwnerThread);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("{}: state change {} -> {}, lock owner thread change {} -> {}{}", this, state, s, lockOwnerThread,
                newLockOwnerThread, clear ? " (lock cleared)" : "");
        }
        state = s;
        lockOwnerThread = newLockOwnerThread;
        if (clear) {
            Thread currentThread = Thread.currentThread();
            clearThreadId = currentThread.getId();
            clearThreadName = currentThread.getName();
        } else if (lockOwnerThread != null) {
            clearThreadId = 0;
            clearThreadName = null;
        }
        signalAll();
    }
    
    
    public void lock() throws InterruptedException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("lock() called on {}", this);
        }
        Thread currentThread = Thread.currentThread();
        
        if (currentThread == lockOwnerThread) {
            throw new IllegalStateException(this + ": Lock is already held by current thread");
        }
        while (!state.canAcquire) { 
            if (LOG.isTraceEnabled()) {
                LOG.trace("waiting to lock() on {}", this);
            }
            await();
        }
        setState(State.LOCKED, currentThread, false);
    }
    
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("tryLock(...) called on {}", this);
        }
        final long end = System.currentTimeMillis() + unit.toMillis(time);
        Thread currentThread = Thread.currentThread();
        if (currentThread == lockOwnerThread) {
            throw new IllegalStateException(this + ": Lock is already held by current thread");
        }
        long rem = 1; 
        while (!state.canAcquire && (rem = end - System.currentTimeMillis()) > 0) { 
            if (LOG.isTraceEnabled()) {
                LOG.trace("waiting for {} ms to tryLock(...) on {}", rem, this);
            }
            await(rem);
        }
        if (rem <= 0) {
            
            if (LOG.isTraceEnabled()) {
                LOG.trace("tryLock(...) timed out on {}", this);
            }
            return false;
        }
        setState(State.LOCKED, currentThread, false);
        return true;
    }
    
    public void unlock() {
        if (LOG.isTraceEnabled()) {
            LOG.trace("unlock() called on {}", this);
        }
        Thread currentThread = Thread.currentThread();
        if (lockOwnerThread != currentThread) {
            if (LOG.isWarnEnabled() && clearThreadId != currentThread.getId()) {
                LOG.warn("Cannot unlock() on {} - Lock is not held by the current thread\nStack trace:\n{}", this,
                    StackTraceUtils.currentStackTraceToString());
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("Cannot unlock() on {} - Lock is not held by the current thread - it was just cleared by it\n"
                        + "Stack trace:\n{}", this, StackTraceUtils.currentStackTraceToString());
            }
            
            return;
        }
        if (state != State.LOCKED && state != State.LOCKED_SCHEDULED && state != State.LOCKED_FOR_RESET) {
            throw new IllegalStateException("Cannot unlock() on " 
                + this
                + " - state must be LOCKED, LOCKED_SCHEDULED or LOCKED_FOR_RESET - was " + state);
        }
        if (state == State.LOCKED || state == State.LOCKED_FOR_RESET) {
            setState(State.NOT_LOCKED, null, false);
        } else { 
            setState(State.SCHEDULED, null, false);
        }
    }
    
    @CheckReturnValue
    public int scheduleAsyncEvent() {
        if (LOG.isTraceEnabled()) {
            LOG.trace("scheduleAsyncEvent() called on {}", this);
        }
        if (lockOwnerThread != Thread.currentThread()) {
            throw new IllegalStateException(
                "Cannot scheduleAsyncEvent() on " + this + " - lock is not held by current thread");
        }
        setState(State.LOCKED_SCHEDULED, lockOwnerThread, false); 
        asyncEventId++;
        if (LOG.isTraceEnabled()) {
            LOG.trace("{}: Async event scheduled ({})", this, asyncEventId);
        }
        return asyncEventId;
    }
    
    public boolean lockAsync(int eventId) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("lockAsync({}) called on {}", eventId, this);
        }
        if (asyncEventId != eventId) {
            LOG.debug("Cannot lockAsync({}) on {} - expected event id={} but have={}", eventId, this, eventId,
                asyncEventId);
            return false;
        }
        Thread currentThread = Thread.currentThread();
        if (currentThread == lockOwnerThread) {
            throw new IllegalStateException("Cannot lockAsync(" + eventId + ") on " + this
                + " - Lock is already held by current thread");
        }
        while (!state.canAsyncAcquire) { 
            try {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("waiting to lockAsync({}) on {}", eventId, this);
                }
                await();
                if (asyncEventId != eventId) {
                    LOG.debug("Cannot lockAsync({}) on {} - Event has been cancelled while waiting id={} but have={}",
                        eventId, this, eventId, asyncEventId);
                    return false;
                }
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while lockAsync() state={} lockOwnerThread={} asyncEventId={} eventId={}", state,
                    lockOwnerThread, asyncEventId, eventId);
                Thread.currentThread().interrupt();
                return false;
            }
        }
        setState(State.LOCKED_ASYNC, currentThread, false);
        return true;
    }
    public void unlockAsync() {
        if (LOG.isTraceEnabled()) {
            LOG.trace("unlockAsync() called on {}", this);
        }
        Thread currentThread = Thread.currentThread();
        if (lockOwnerThread != currentThread) {
            if (LOG.isWarnEnabled() && clearThreadId != currentThread.getId()) {
                LOG.warn("Cannot unlockAsync() on {} - Lock is not held by the current thread\nStack trace:\n{}", this,
                    StackTraceUtils.currentStackTraceToString());
            } else if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Cannot unlockAsync() on {} - Lock is not held by the current thread - it was just cleared by it\n"
                        + "Stack trace:\n{}", this, StackTraceUtils.currentStackTraceToString());
            }
            
            return;
        }
        if (!(state == State.LOCKED_ASYNC || state == State.LOCKED_SCHEDULED || state == State.LOCKED_FOR_RESET)) {
            throw new IllegalStateException("Invalid state for unlockAsync() on " + this
                + " - should be LOCKED_ASYNC, LOCKED_SCHEDULED or LOCKED_FOR_RESET");
        }
        if (state == State.LOCKED_SCHEDULED) {
            setState(State.SCHEDULED, null, false);
        } else {
            setState(State.NOT_LOCKED, null, false);
            
            
            ++asyncEventId;
            if (LOG.isTraceEnabled()) {
                LOG.trace("async event invalidated in unlockAsync() on {}, new id is {}", this, asyncEventId);
            }
        }
    }
    public void unlockAndReschedule() {
        if (LOG.isTraceEnabled()) {
            LOG.trace("unlockAndReschedule() called on {}", this);
        }
        Thread currentThread = Thread.currentThread();
        if (lockOwnerThread != currentThread) {
            if (LOG.isWarnEnabled() && clearThreadId != currentThread.getId()) {
                LOG.warn(
                    "Cannot unlockAndReschedule() on {} - Lock is not held by the current thread\nStack trace:\n{}",
                    this,
                    StackTraceUtils.currentStackTraceToString());
            } else if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Cannot unlockAndReschedule() on {} - Lock is not held by the current thread - "
                        + "it was just cleared by it\nStack trace:\n{}", this,
                    StackTraceUtils.currentStackTraceToString());
            }
            
            return;
        }
        if (state != State.LOCKED_ASYNC) {
            throw new IllegalStateException(
                "Cannot unlockAndReschedule() on " + this + " - state must be LOCKED_ASYNC, but was " + state);
        }
        setState(State.SCHEDULED, null, false);
    }
    protected void clear(Thread currentThread) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("clear({}) called on {}", currentThread, this);
        }
        if (state == State.NOT_LOCKED) {
            
            if (LOG.isTraceEnabled()) {
                LOG.trace("No need to clear({}) in {}", currentThread, this);
            }
            return;
        }
        if (lockOwnerThread == currentThread && state == State.LOCKED_FOR_RESET) {
            
            LOG.debug("Cannot clear({}) in {}", currentThread, this);
            return;
        }
        try {
            while (lockOwnerThread != null && lockOwnerThread != currentThread) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Waiting to clear({}) in {}", currentThread, this);
                }
                await();
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while cleanup");
            Thread.currentThread().interrupt(); 
        }
        if (state == State.NOT_LOCKED) {
            
            if (LOG.isTraceEnabled()) {
                LOG.trace("No need to clear({}) in {} after waiting on the lock", currentThread, this);
            }
            return;
        }
        setState(State.NOT_LOCKED, null, true);
        
        ++asyncEventId;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Async event invalidated in clear({}) in {}, new id is {}", currentThread, this, asyncEventId);
        }
    }
    
    public void clear() {
        Thread currentThread = Thread.currentThread();
        clear(currentThread);
    }
    public boolean hasScheduledEvent() {
        return state == State.LOCKED_SCHEDULED || state == State.SCHEDULED;
    }
    
    @CheckReturnValue
    public boolean lockForReset() {
        if (LOG.isTraceEnabled()) {
            LOG.trace("lockForReset() called on {}", this);
        }
        Thread currentThread = Thread.currentThread();
        try {
            while (lockOwnerThread != null && lockOwnerThread != currentThread) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("waiting to lockForReset() on {}", this);
                }
                await();
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while locking for cleanup");
            Thread.currentThread().interrupt(); 
            return false;
        }
        boolean notHeld = lockOwnerThread == null;
        if (LOG.isTraceEnabled()) {
            LOG.trace("lockForReset() lockOwnerThread transition {} -> {} on {}", lockOwnerThread, currentThread, this);
        }
        if (notHeld) {
            setState(State.LOCKED_FOR_RESET, currentThread, false);
            ++asyncEventId; 
            if (LOG.isDebugEnabled()) {
                LOG.debug("async event invalidated in lockForReset() on {}, new id is {}", this, asyncEventId);
            }
        }
        
        return notHeld;
    }
    
    public void releaseResetLock() {
        if (LOG.isTraceEnabled()) {
            LOG.trace("releaseResetLock() called on {}", this);
        }
        Thread currentThread = Thread.currentThread();
        if (lockOwnerThread != currentThread || state != State.LOCKED_FOR_RESET) {
            if (LOG.isWarnEnabled() && clearThreadId != currentThread.getId()) {
                LOG.warn(
                    "Cannot releaseResetLock() on {} - Lock is not held by the current thread or not locked for reset\n"
                        + "Stack trace:\n{}", this, StackTraceUtils.currentStackTraceToString());
            } else if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Cannot releaseResetLock() on {} - Lock is not held by the current thread or not locked for reset"
                        + " - it was just cleared by it\n"
                        + "Stack trace:\n{}", this, StackTraceUtils.currentStackTraceToString());
            }
            
            return;
        }
        setState(State.NOT_LOCKED, null, false);
    }
    
    public interface Unlock extends AutoCloseable {
        @Override
        void close();
        Unlock NOOP = () -> {};
    }
    
    @CheckReturnValue
    public abstract Unlock acquireLock() throws InterruptedException;
    
    public abstract void whileHolding(Runnable block);
    @VisibleForTesting
    abstract ReentrantLock internalLock();
    public boolean isHeldByCurrentThread() {
        Thread currentThread = Thread.currentThread();
        return lockOwnerThread == currentThread;
    }
    
    public boolean assertsLockedByCurrentThread() {
        return isHeldByCurrentThread();
    }
    
    public boolean notHeldByCurrentThread() {
        return !isHeldByCurrentThread();
    }
    
    public State getState() {
            return state;
    }
    
    public Thread getLockOwnerThread() {
        return lockOwnerThread;
    }
    
    public int getAsyncEventId() {
        return asyncEventId;
    }
    @Override
    public String toString() {
        return "ConnectingLock (" + Integer.toHexString(System.identityHashCode(this)) + ") state=" + state
            + " lockOwnerThread=" + lockOwnerThread + " clearThreadName=" + clearThreadName + " clearThreadId="
            + clearThreadId;
    }
}
