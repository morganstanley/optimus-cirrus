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
package msjava.base.spring.lifecycle;
import org.springframework.beans.factory.FactoryBean;
import com.google.common.base.Throwables;
public final class BeanState {
    private boolean running;
    private Class<? extends Runnable> initializer;
    private Class<? extends Runnable> starter;
    private Class<? extends Runnable> stopper;
    private Class<? extends Runnable> destroyer;
    private static final Runnable NOOP = new Runnable() {
        public void run() {
        };
    };
    
    public synchronized void throwIfInactive() {
        if (initializer == null) {
            throw new IllegalStateException("Not yet initialized");
        }
        if (destroyer != null) {
            throw new IllegalStateException("Already disposed");
        }
    }
    
    public synchronized void throwIfNotRunning() {
        if (!running || !isActive()) {
            throw new IllegalStateException("Not running");
        }
    }
    
    public synchronized void throwIfDestroyed() {
        if (destroyer != null) {
            throw new IllegalStateException("Already destroyed");
        }
    }
    
    public synchronized void throwIfInitialized() {
        if (initializer != null) {
            throw new IllegalStateException("Already initialized");
        }
    }
    
    @Deprecated
    public synchronized void destroyed() {
        destroyIfNotDestroyed();
    }
    public synchronized void destroyIfNotDestroyed() {
        destroyIfNotDestroyed(NOOP);
    }
    
    public synchronized void destroyIfNotDestroyed(Runnable runnable) {
        if (destroyer == null) {
            try {
                runnable.run();
                destroyer = runnable.getClass();
            } catch (Throwable throwable) {
                Throwables.throwIfUnchecked(throwable);
                throw new RuntimeException(throwable);
            }
        }
        if (runnable.getClass() != destroyer) {
            throw new IllegalArgumentException("Already destroyed with a different instance of Runnable");
        }
        
    }
    
    public synchronized void stopIfRunningCompletionListener(Runnable completionListener) {
        stopIfRunningCompletionListener(NOOP, completionListener);
    }
    
    public synchronized void stopIfRunning() {
        stopIfRunningCompletionListener(null);
    }
    
    public synchronized void stopIfRunningCompletionListener(Runnable runnable, Runnable completionListener) {
        if (stopper != null && runnable.getClass() != stopper) {
            throw new IllegalArgumentException("Already stopped with a different instance of Runnable");
        }
        if (running) {
            try {
                runnable.run();
                stopper = runnable.getClass();
                running = false;
            } catch (Throwable throwable) {
                Throwables.throwIfUnchecked(throwable);
                throw new RuntimeException(throwable);
            }
        }
        
        if (completionListener != null) {
            completionListener.run();
        }
    }
    
    public synchronized void stopIfRunning(Runnable runnable) {
        stopIfRunningCompletionListener(runnable, null);
    }
    
    @Deprecated
    public synchronized void initialized() {
        throwIfInitialized();
        throwIfDestroyed();
        initializer = NOOP.getClass();
    }
    
    public synchronized void startIfNotRunning() {
        startIfNotRunning(NOOP);
    }
    
    public synchronized void initializeIfNotInitialized(Runnable runnable) {
        throwIfDestroyed();
        if (initializer == null) {
            try {
                runnable.run();
                initializer = runnable.getClass();
            } catch (Throwable throwable) {
                Throwables.throwIfUnchecked(throwable);
                throw new RuntimeException(throwable);
            }
        }
        if (runnable.getClass() != initializer) {
            throw new IllegalArgumentException("Already initialized with a different instance of Runnable");
        }
        
    }
    
    public synchronized void initializeIfNotInitialized() {
        initializeIfNotInitialized(NOOP);
    }
    
    public synchronized void startIfNotRunning(Runnable runnable) {
        throwIfInactive();
        if (starter != null && runnable.getClass() != starter) {
            throw new IllegalArgumentException("Already started with a different instance of Runnable");
        }
        if (!running) {
            try {
                runnable.run();
                starter = runnable.getClass();
                running = true;
            } catch (Throwable throwable) {
                Throwables.throwIfUnchecked(throwable);
                throw new RuntimeException(throwable);
            }
        }
        
    }
    
    
    public synchronized boolean isDestroyed() {
        return destroyer != null;
    }
    
    public synchronized boolean isInitialized() {
        return initializer != null;
    }
    
    public synchronized boolean isActive() {
        return initializer != null && destroyer == null;
    }
    
    public synchronized boolean isRunning() {
        return running;
    }
    
    public synchronized boolean isStopped() {
        return stopper != null;
    }
    
    public synchronized boolean isStarted() {
        return starter != null;
    }
}