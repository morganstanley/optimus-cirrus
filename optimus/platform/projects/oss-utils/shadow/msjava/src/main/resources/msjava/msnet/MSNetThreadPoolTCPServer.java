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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import msjava.base.annotation.Experimental;
import msjava.base.lang.StackTraceUtils;
import msjava.msnet.MSNetLoop.MSNetLoopErrorHandler;
import msjava.msnet.MSNetLoop.MSNetLoopErrorHandlingStrategy;
import msjava.msnet.MSNetLoop.MaxLoopRestartsErrorHandler;
import msjava.msnet.utils.MSNetConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
@ManagedResource(description = "The MSNetThreadPoolServer is an implementation of MSNetTCPServer that has a pool of n threads, each with it's own MSNetLoop, that the connections is manages get run on.")
public class MSNetThreadPoolTCPServer extends MSNetAbstractTCPServer {
	
	public static final int DEFAULT_POOL_SIZE = 1;
	
	@Deprecated
	public static final int INFINITE_LOOP_RESTARTS = -1;
	
	@Deprecated
	protected int _loopShutdownTimeout = MSNetConfiguration.DEFAULT_LOOP_SHUTDOWN_TIMEOUT;
	
	@Deprecated
	protected int _size;
	
	@Deprecated
	protected int _maxLoopRestarts = INFINITE_LOOP_RESTARTS;
	
	protected ThreadPoolExecutor _pool;
	
	protected MSNetLoop[] _loops;
	
	protected int _loopIndexCount;
    
    private MSNetPoolThreadCreatedListener poolThreadCreatedListener;
	private static final Logger LOGGER = LoggerFactory.getLogger(MSNetThreadPoolTCPServer.class);
	
	
	public MSNetThreadPoolTCPServer() {
	    this.configurationSettings = new DefaultConfiguration(null, null, null, new MSNetStringTCPConnectionFactory(), 
	            DEFAULT_POOL_SIZE);
	}
	
	public MSNetThreadPoolTCPServer(MSNetLoop loop, MSNetInetAddress address, MSNetID serverName) {
		this(loop, address, serverName, new MSNetPlainTCPConnectionFactory(), DEFAULT_POOL_SIZE);
	}
	
	public MSNetThreadPoolTCPServer(MSNetLoop loop, MSNetInetAddress address, MSNetID serverName, int size_) {
		this(loop, address, serverName, new MSNetPlainTCPConnectionFactory(), size_);
	}
	
	public MSNetThreadPoolTCPServer(MSNetLoop loop, MSNetInetAddress address, MSNetID serverName,
			MSNetTCPConnectionFactory connectionFactory) {
		this(loop, address, serverName, connectionFactory, DEFAULT_POOL_SIZE);
	}
	
	public MSNetThreadPoolTCPServer(MSNetLoop loop, MSNetInetAddress address, MSNetID serverName,
			MSNetTCPConnectionFactory connectionFactory, int size) {
		this(loop, address, serverName, connectionFactory, true, size);
	}
	
	public MSNetThreadPoolTCPServer(MSNetLoop loop, MSNetInetAddress address, MSNetID serverName,
			MSNetTCPConnectionFactory connectionFactory, boolean forwardEvents, int size) {
		this.configurationSettings = new DefaultConfiguration(loop, address, serverName, connectionFactory, size);
		setForwardAcceptedEvents(forwardEvents);
	}
	
	public MSNetThreadPoolTCPServer(Configuration configuration) {
		super(configuration);
		configuration.setServer(this);
	}
    
    @Deprecated
    public void setMaxLoopRestarts(int maxLoopRestarts) {
        getConfigurationSettings().setMaxLoopRestarts(maxLoopRestarts);
    }
    
    @Deprecated
    public int getMaxLoopRestarts() {
        return getConfigurationSettings().getMaxLoopRestarts();
    }
    
    public MSNetLoopErrorHandler getLoopErrorHandlerForPool() {
        return getConfigurationSettings().getLoopErrorHandlerForPool();
    }
    
    public void setLoopErrorHandlerForPool(MSNetLoopErrorHandler loopErrorHandler) {
        getConfigurationSettings().setLoopErrorHandlerForPool(loopErrorHandler);
        if (_loops != null) {
            for (MSNetLoop loop : _loops) {
                if (loop != null) {
                    loop.setLoopErrorHandler(loopErrorHandler);
                }
            }
        }
    }
	
	protected void _initLoopHandles(int offset, int num) {
		for (int i = offset; i < offset + num; ++i) {
			_loops[i] = new MSNetLoop(null);
			_loops[i].setLoopErrorHandler(getLoopErrorHandlerForPool());
		}
	}
	
	protected void _startThreadPool(int size) {
		if (size == 0) {
			return;
		}
		
		
		int keepAliveTime = 0;
		_loops = new MSNetLoop[size];
		
		_pool = createThreadPoolExecutor(size, keepAliveTime);
		_pool.prestartAllCoreThreads();
		
		
		
		_initLoopHandles(0, size);
		_startThreads(0, size);
	}
	private ThreadPoolExecutor createThreadPoolExecutor(int size, int keepAliveTime) {
		ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
		threadPoolTaskExecutor.setCorePoolSize(size);
		threadPoolTaskExecutor.setMaxPoolSize(Integer.MAX_VALUE);
		threadPoolTaskExecutor.setKeepAliveSeconds(keepAliveTime/1000);
		threadPoolTaskExecutor.setQueueCapacity(Integer.MAX_VALUE);
		threadPoolTaskExecutor.setBeanName(this.getName() != null ? this.getName().toString()
				+ "srvPool" : "srvPool");
		threadPoolTaskExecutor.afterPropertiesSet();
		return threadPoolTaskExecutor.getThreadPoolExecutor();
	}
	
	protected void _startThreads(int offset, int num) {
		
		
		final CountDownLatch countDown = new CountDownLatch(num);
		try {
			
			
			for (int i = offset; i < offset + num; ++i) {
				final int threadNum = i;
				final MSNetLoop loop = _loops[i];
                _pool.execute(new Runnable() {
                    public void run() {
                        final AtomicBoolean coundownCalled = new AtomicBoolean();
                        int count = 0;
                        boolean err = true;
                        try {
                            Thread.currentThread().setName(getName() + "-serverPoolThread#" + threadNum);
                            notifyPoolThreadCreated(threadNum);
                            
                            loop.setImpl(new MSNetLoopDefaultImpl());
                            while (err
                                    && ((getMaxLoopRestarts() == INFINITE_LOOP_RESTARTS) || (count <= getMaxLoopRestarts()))) {
                                
                                
                                if (count > 0) {
                                    if (LOGGER.isInfoEnabled()) {
                                        LOGGER.info("Restarting loop {} (#{}), restart #{}", new Object[] {
                                                Thread.currentThread().toString(), loop.hashCode(), count });
                                    }
                                }
                                try {
                                    err = false;
                                    
                                    loop.callbackAfterDelay(0, new MSNetEventListener() {
                                        public void eventOccurred(MSNetEvent e) {
                                            countDown.countDown();
                                            coundownCalled.set(true);
                                        }
                                    });
                                    loop.setContinueLoop(true);
                                    loop.loop();
                                } catch (MSNetFatalException e) {
                                    
                                    LOGGER.error("Loop " + loop.hashCode()
                                            + " died with fatal exception. Will not reloop", e);
                                    notifyServerError(new MSNetServerException("Loop " + loop.hashCode()
                                            + " died with fatal exception", e));
                                } catch (Throwable t_) {
                                    ++count;
                                    err = true;
                                    LOGGER.warn("Caught unchecked Throwable that caused loop " + loop.hashCode()
                                            + " to exit", t_);
                                }
                            }
                            if (err && (count != INFINITE_LOOP_RESTARTS) && (count > getMaxLoopRestarts())) {
                                LOGGER.error("Loop {} died too many times. Will not reloop.", loop.hashCode());
                                notifyServerError(new MSNetServerException("Loop " + loop.hashCode()
                                        + " died too many times"));
                            }
                        } catch (Exception e) {
                            LOGGER.error("Failed to start loop " + loop.hashCode(), e);
                            notifyServerError(new MSNetServerException("Failed to start loop " + loop.hashCode(), e));
                        } finally {
                            
                            if (!coundownCalled.get()) {
                                countDown.countDown();
                            }
                        }
                    }
                });
            }
			
			
			countDown.await();
		} catch (InterruptedException propagate) {
			
			
			Thread.currentThread().interrupt();
		}
	}
	
	@Override
	protected void _start() {
		_startThreadPool(getConfigurationSettings().getPoolSize());
		super._start();
	}
	
	protected void _stopThreadPool() {
		if (getConfigurationSettings().getPoolSize() > 0) {
			if (null != _pool) {
				_pool.shutdown();
			}
			MSNetLoop l[] = _loops;
			if (l != null) {
				_stopThreads(0, l.length);
			}
			
			
			
			
			if (null != _pool) {
				_pool.shutdownNow();
			}
		}
		_loops = null;
		_pool = null;
	}
	
	protected void _stopThreads(int offset, int num) {
		if (_loops == null) {
			
			return;
		}
        for (int i = offset; i < offset + num; ++i) {
            final MSNetLoop loop = _loops[i];
            if (null != loop && loop.isLooping()) {
                loop.quit();
                Thread t = MSNetLoop.getThreadForLoop(loop);
                if (t == Thread.currentThread()) {
                    
                } else if (t != null) {
                    try {
                        t.join(getConfigurationSettings().getLoopShutdownTimeout());
                    } catch (InterruptedException e) {
                        
                    }
                    if (t.isAlive()) {
                        LOGGER.info("Had to forcibly quit loop thread {} (#{})", t.getName(), i);
                        if (LOGGER.isDebugEnabled()){
                            StackTraceUtils.logAllStackTraces(LOGGER);
                        }
                        t.interrupt();
                    }
                }
            }
        }
	}
	
	@Override
	protected void _stop(boolean closeAllConnection) {
		super._stop(closeAllConnection);
		_stopThreadPool();
	}
	
	public int getPoolSize() {
		return getConfigurationSettings().getPoolSize();
	}
	
	public void setPoolSize(int newSize) {
		getConfigurationSettings().setPoolSize(newSize);
	}
	
	public void rebalance() {
		doAcquire(writeLock());
		try {
			_rebalance(_getConnectionContexts(), 0, getConfigurationSettings().getPoolSize());
		} finally {
			writeLock().unlock();
		}
	}
	
    protected void _rebalance(final List<MSNetConnectionContext> contexts_, final int offset, final int num) {
        assert num == 0 || num + offset <= _loops.length;
        
        LOGGER.debug("Rebalancing {} connections to {} loops", contexts_.size(), num);
        int idx = 0;
        for (MSNetConnectionContext ctxt : contexts_) {
            MSNetLoop loop;
            if (num == 0) {
                loop = getNetLoop();
            } else {
                loop = _loops[offset + ((idx++) % num)];
            }
            ctxt.reparent(loop);
        }
    }
	
	
	@Override
	protected MSNetConnectionContext createManagedConnectionContext(MSNetTCPConnection connection, MSNetLoop loop) {
		connection.reparent(loop);
		MSNetConnectionContext context = createConnectionContext(loop, connection,
				MSNetConnectionContextTypeEnum.MANAGED_CONNECTION);
		return context;
	}
	
	@Override
	protected MSNetConnectionContext createOpenedConnectionContext(MSNetTCPConnection connection, MSNetLoop loop) {
		connection.reparent(loop);
		MSNetConnectionContext context = createConnectionContext(loop, connection,
				MSNetConnectionContextTypeEnum.OPENED_CONNECTION);
		return context;
	}
	
	@Override
	protected MSNetConnectionContext createAcceptedConnectionContext(MSNetTCPSocket socket_, MSNetLoop loop_) {
		MSNetID name = getNextID(socket_);
		MSNetTCPConnection connection = getConnectionFactory().createConnection(loop_, socket_, name, false);
		connection.setIdleTimeout(idleTimeout);
        connection.setAsyncConnectTimeout(connectTimeout);
		MSNetConnectionContext context = createConnectionContext(loop_, connection,
				MSNetConnectionContextTypeEnum.ACCEPTED_CONNECTION);
		return context;
	}
	
	protected int getNextLoopIndex() {
		return _loopIndexCount = (_loopIndexCount + 1) % getConfigurationSettings().getPoolSize();
	}
	
	@Override
	protected MSNetLoop getNetLoopForConnection() {
		MSNetLoop rv = null;
		MSNetLoop loops[] = _loops;
		if (null == loops || getConfigurationSettings().getPoolSize() == 0) {
			rv = getNetLoop();
		} else {
			rv = loops[getNextLoopIndex()];
		}
		return rv;
	}
	public int getLoopShutdownTimeout() {
		return getConfigurationSettings().getLoopShutdownTimeout();
	}
	public void setLoopShutdownTimeout(int loopShutdownTimeout) {
		getConfigurationSettings().setLoopShutdownTimeout(loopShutdownTimeout);
	}
    
	@Experimental
    public void setPoolThreadCreatedListener(MSNetPoolThreadCreatedListener poolThreadCreatedListener) {
        if (this.poolThreadCreatedListener != null && LOGGER.isDebugEnabled()) {
            LOGGER.debug("Changing poolThreadCreatedListener {} -> {}", this.poolThreadCreatedListener,
                    poolThreadCreatedListener);
        }
        this.poolThreadCreatedListener = poolThreadCreatedListener;
    }
    private void notifyPoolThreadCreated(int threadNum) {
        MSNetPoolThreadCreatedListener poolThreadCreatedListener = this.poolThreadCreatedListener;
        if (poolThreadCreatedListener != null) {
            poolThreadCreatedListener.poolThreadCreated(threadNum);
        }
    }
    
    public static interface MSNetPoolThreadCreatedListener {
        
        void poolThreadCreated(int threadNum);
    }
	static interface MSNetThreadPoolTCPServerConfiguration {
		
		public static final int INFINITE_LOOP_RESTARTS = -1;
        
        @Deprecated
        public abstract void setMaxLoopRestarts(int maxLoopRestarts);
        
        @Deprecated
        public abstract int getMaxLoopRestarts();
        
        public abstract void setLoopErrorHandlerForPool(MSNetLoopErrorHandler loopErrorHandler);
        
        public abstract MSNetLoopErrorHandler getLoopErrorHandlerForPool();
		public abstract int getLoopShutdownTimeout();
		public abstract void setLoopShutdownTimeout(int loopShutdownTimeout);
		public abstract void setPoolSize(int newSize);
		
		public abstract int getPoolSize();
	}
	
	public static class Configuration extends msjava.msnet.MSNetAbstractTCPServer.Configuration implements
			MSNetThreadPoolTCPServerConfiguration {
		private MSNetThreadPoolTCPServer server;
		
		protected int maxLoopRestarts = INFINITE_LOOP_RESTARTS;
		public int loopShutdownTimeout = MSNetConfiguration.DEFAULT_LOOP_SHUTDOWN_TIMEOUT;
		private int poolSize = -1;
        private MSNetLoopErrorHandler loopErrorHandlerForPool = MSNetConfiguration.getDefaultLoopErrorHandler();
		
		protected Configuration() {
		}
		
        @Deprecated
		@Override
		public final void setMaxLoopRestarts(int maxLoopRestarts) {
			this.maxLoopRestarts = maxLoopRestarts;
		}
		
        @Deprecated
		@Override
		public final int getMaxLoopRestarts() {
			return maxLoopRestarts;
		}
        
        
        @Override
        public void setLoopErrorHandlerForPool(MSNetLoopErrorHandler loopErrorHandler) {
            if (loopErrorHandler == null) {
                loopErrorHandlerForPool = MSNetConfiguration.getDefaultLoopErrorHandler();
            } else {
                loopErrorHandlerForPool = loopErrorHandler;
            }
        }
        
        @Override
        public MSNetLoopErrorHandler getLoopErrorHandlerForPool() {
            return loopErrorHandlerForPool ;
        }
		@Override
		public final int getLoopShutdownTimeout() {
			return loopShutdownTimeout;
		}
		@Override
		public final void setLoopShutdownTimeout(int loopShutdownTimeout) {
			this.loopShutdownTimeout = loopShutdownTimeout;
		}
		@Override
		@ManagedAttribute(description = "the current size of the thread pool.")
		public final void setPoolSize(int newSize) {
			if (server != null) {
				server.resizePool(newSize);
			}
			this.poolSize = newSize;
		}
		
		@Override
		@ManagedAttribute(description = "the current size of the thread pool.")
		public final int getPoolSize() {
			return this.poolSize;
		}
		private final void setServer(MSNetThreadPoolTCPServer server) {
			if (this.server != null) {
				throw new UnsupportedOperationException(
						"A Server is tied to the configuration already, please use a different configuration instance");
			}
			this.server = server;
		}
		
		public MSNetThreadPoolTCPServer getServer() {
            return server;
        }
		
		@Override
		protected void validate() {
			super.validate();
			MSNetThreadPoolTCPServer.validate(this);
		}
	}
	private class DefaultConfiguration extends msjava.msnet.MSNetAbstractTCPServer.DefaultConfiguration implements
			MSNetThreadPoolTCPServerConfiguration {
		private MSNetLoopErrorHandler loopErrorHandlerForPool;
        public DefaultConfiguration(MSNetLoop loop, MSNetInetAddress address, MSNetID serverName,
				MSNetTCPConnectionFactory connectionFactory, int size) {
			super(loop, address, serverName, null, connectionFactory, null, null);
			_size = size;
		}
		@Override
		public int getLoopShutdownTimeout() {
			return _loopShutdownTimeout;
		}
		
        @Deprecated
		@Override
		public int getMaxLoopRestarts() {
			return _maxLoopRestarts;
		}
		@Override
		public int getPoolSize() {
			return _size;
		}
        
        @Override
        public MSNetLoopErrorHandler getLoopErrorHandlerForPool() {
            return loopErrorHandlerForPool ;
        }
		
        
        @Override
        public void setLoopErrorHandlerForPool(MSNetLoopErrorHandler loopErrorHandler) {
            if (loopErrorHandler == null) {
                loopErrorHandlerForPool = MSNetConfiguration.getDefaultLoopErrorHandler();
            } else {
                loopErrorHandlerForPool = loopErrorHandler;
            }
        }
		@Override
		public void setLoopShutdownTimeout(int loopShutdownTimeout) {
			_loopShutdownTimeout = loopShutdownTimeout;
		}
		
        @Deprecated
		@Override
		public void setMaxLoopRestarts(int maxLoopRestarts) {
			_maxLoopRestarts = maxLoopRestarts;
		}
		@Override
		public void setPoolSize(int newSize) {
			resizePool(newSize);
		}
		
		@Override
		protected void validate() {
			super.validate();
			MSNetThreadPoolTCPServer.validate(this);
		}
	}
	
	private static void validate(MSNetThreadPoolTCPServerConfiguration configuration) {
		if (configuration.getPoolSize() < 0) {
			throw new IllegalArgumentException("MSNetThreadPoolTCPServer.<ctor>: Size must be a non negative integer. Either set it using setPoolSize or look call afterPropertiesSet(.) to pick up the defaults");
		}
	}
	private void resizePool(int newSize) {
		if (!isStarted()) {
			this._size = newSize;
			return;
		}
		LOGGER.info("Setting pool size {} => {}", getConfigurationSettings().getPoolSize(), newSize);
		if (newSize < 0) {
			throw new IllegalArgumentException("MSNetThreadPoolTCPServer.setPoolSize(): Size must be >= 0.");
		}
		doAcquire(writeLock());
		try {
			List<MSNetConnectionContext> contexts = _getConnectionContexts();
			int oldSize = getConfigurationSettings().getPoolSize();
			if (newSize == oldSize) {
				
				return;
			}
			int diff = newSize - oldSize;
			if (diff > 0) 
			{
				if (oldSize == 0) {
					_startThreadPool(newSize);
				} else {
					
					MSNetLoop[] oldLoops = _loops;
					_loops = new MSNetLoop[newSize];
					System.arraycopy(oldLoops, 0, _loops, 0, oldLoops.length);
					_initLoopHandles(oldSize, diff);
					
					_pool.setMaximumPoolSize(newSize);
					_pool.setCorePoolSize(newSize);
					_startThreads(oldSize, diff);
				}
				
				_rebalance(contexts, 0, newSize);
			} else 
			{
				
				
				diff *= -1;
				
				
				_rebalance(contexts, 0, newSize);
				if (newSize == 0) 
				{
					_stopThreadPool();
				} else {
					
					_pool.setCorePoolSize(newSize);
					_pool.setMaximumPoolSize(newSize);
					
					_stopThreads(newSize, diff);
					MSNetLoop[] oldLoops = _loops;
					_loops = new MSNetLoop[newSize];
					System.arraycopy(oldLoops, 0, _loops, 0, newSize);
				}
			}
			_size = newSize;
		} finally {
			doUnlock(writeLock());
		}
	}
	private MSNetThreadPoolTCPServerConfiguration getConfigurationSettings() {
		return (MSNetThreadPoolTCPServerConfiguration) configurationSettings;
	}
}
