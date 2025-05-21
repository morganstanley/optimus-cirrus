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
package msjava.base.dns;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import msjava.base.slf4j.ContextLogger;
import msjava.base.util.internal.SystemPropertyUtils;
public class AsyncNameService implements MSNameService {
	private static final Logger logger = ContextLogger.safeLogger();
	
	public static final String TIMEOUT_MS_PROPERTY = "msjava.base.dns.async.timeout_ms";
	public static final long TIMEOUT_MS_DEFAULT = 5000;
	public static final String WARN_MS_PROPERTY = "msjava.base.dns.async.warn_ms";
	public static final long WARN_MS_DEFAULT = 2000;
	public static final String MAX_POOL_SIZE_PROPERTY = "msjava.base.dns.async.maxpoolsize";
	public static final int MAX_POOL_SIZE_DEFAULT = 50;
	public static final String CORE_POOL_SIZE_PROPERTY = "msjava.base.dns.async.corepoolsize";
	public static final int CORE_POOL_SIZE_DEFAULT = 1;
	public static final String QUEUE_SIZE_PROPERTY = "msjava.base.dns.async.queuesize";
	public static final int QUEUE_SIZE_DEFAULT = 0;
	private final MSNameService delegate;
	private final long timeoutMs;
	private final long warnMs;
	
	private final ExecutorService executor; 
	
	
	public AsyncNameService() {
		this(new Builder());
	}
	private AsyncNameService(Builder builder) {
		this.delegate = builder.delegate();
		this.warnMs = builder.warnMs();
		this.timeoutMs = builder.timeoutMs();
		
		int maxPoolSize = builder.maxPoolSize();
		int corePoolSize = builder.corePoolSize();
		int queueSize = builder.queueSize();
		
		logger.debug("Created AsyncNameService with warning {}ms, timeout {}ms, thread pool {} to {}, work queue {}",
				this.warnMs, this.timeoutMs, corePoolSize, maxPoolSize,	queueSize);
		
		BlockingQueue<Runnable> queue = queueSize > 0 ? new LinkedBlockingQueue<>(queueSize) : new SynchronousQueue<>();
		ThreadFactory tfact = new ThreadFactoryBuilder()
				.setNameFormat("nslookup-%d")
				.setDaemon(true)
				.build();
		this.executor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, 60L, TimeUnit.SECONDS, queue, tfact);
	}
	
	@Override
	public InetAddress[] lookupAllHostAddr(String hostname) throws UnknownHostException {
		return submit(() -> delegate.lookupAllHostAddr(hostname), "DNS lookup of " + hostname);
	}
	
	@Override
	public String getHostByAddr(byte[] addr) throws UnknownHostException {
		String readableAddr = InetAddress.getByAddress(addr).getHostAddress(); 
		return submit(() -> delegate.getHostByAddr(addr), "DNS reverse lookup of " + readableAddr);
	}
	
	private <T> T submit(Callable<T> task, String operationDesc) throws UnknownHostException {
		try {
			Future<T> future = executor.submit(task);
			return getResult(future, operationDesc);
		} catch (RejectedExecutionException e) {
			UnknownHostException uhe = new UnknownHostException("Unable to execute " + operationDesc + " - queue likely full: "+  e.getLocalizedMessage());
			uhe.initCause(e);
			throw uhe;
		}
	}
	private <T> T getResult(Future<T> future, String operationDesc) throws UnknownHostException {
		long start = System.currentTimeMillis();
		try {
			return getResultWithTimeout(future, operationDesc, Math.min(warnMs, timeoutMs));
		}
		catch (TimeoutException e) {
			try {
				long waitedFor = System.currentTimeMillis() - start;
				long remaining = timeoutMs - waitedFor;
				if (remaining > 0) {
					logger.warn(operationDesc + " has not completed after {}ms. Still waiting.", waitedFor);
					T result =  getResultWithTimeout(future, operationDesc, remaining);
					logger.warn(operationDesc + " completed after {}ms.", System.currentTimeMillis() - start);
					return result;
				} else {
					cancelAndReportTimeout(future, operationDesc, start, e);
					return null; 
				}
			}
			catch (TimeoutException e1) {
				cancelAndReportTimeout(future, operationDesc, start, e1);
				return null; 
			}
		}
	}
	
	private <T> T getResultWithTimeout(Future<T> future, String operationDesc, long remaining) throws UnknownHostException, TimeoutException {
		try {
			return future.get(remaining, TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			UnknownHostException uhe = new UnknownHostException("Interrupted during " + operationDesc);
			uhe.initCause(e);
			throw uhe;
		}
		catch (ExecutionException e) {
			if (e.getCause() instanceof UnknownHostException) {
				UnknownHostException uhe = new UnknownHostException(e.getCause().getLocalizedMessage());
				uhe.initCause(e);
				throw uhe;
			}
			throw new RuntimeException("Unexpected exception thrown during " + operationDesc, e);
		}
	}
	
	private <T> void cancelAndReportTimeout(Future<T> future, String operationDesc, long start, TimeoutException timeout) throws UnknownHostException {
		long waitedFor = System.currentTimeMillis() - start;
		logger.error(operationDesc + " has timed out after " + waitedFor + "ms. Cancelling.", timeout);
		future.cancel(true);
		UnknownHostException uhe = new UnknownHostException("Timed out after " + timeoutMs + "ms for " + operationDesc);
		uhe.initCause(timeout);
		throw uhe;
	}
	
	
	public static class Builder {
		
		private MSNameService delegate;
		private long timeoutMs = SystemPropertyUtils.getLong(TIMEOUT_MS_PROPERTY, TIMEOUT_MS_DEFAULT, logger);
		private long warnMs = SystemPropertyUtils.getLong(WARN_MS_PROPERTY, WARN_MS_DEFAULT, logger);
		private int maxPoolSize = SystemPropertyUtils.getInteger(MAX_POOL_SIZE_PROPERTY, MAX_POOL_SIZE_DEFAULT, logger);
		private int corePoolSize = SystemPropertyUtils.getInteger(CORE_POOL_SIZE_PROPERTY, CORE_POOL_SIZE_DEFAULT, logger);
		private int queueSize = SystemPropertyUtils.getInteger(QUEUE_SIZE_PROPERTY, QUEUE_SIZE_DEFAULT, logger);
		
		public Builder delegate(MSNameService delegate) {
			this.delegate = delegate;
			return this;
		}
		
		public Builder timeoutMs(long timeoutMs) {
			this.timeoutMs = timeoutMs;
			return this;
		}
		
		public Builder warnMs(long warnMs) {
			this.warnMs = warnMs;
			return this;
		}
		
		public Builder maxPoolSize(int maxPoolSize) {
			this.maxPoolSize = maxPoolSize;
			return this;
		}
		
		public Builder corePoolSize(int corePoolSize) {
			this.corePoolSize = corePoolSize;
			return this;
		}
		
		public Builder queueSize(int queueSize) {
			this.queueSize = queueSize;
			return this;
		}
		public MSNameService delegate() {
			return delegate == null ? new JDKNameServiceWrapper() : delegate;
		}
		public long timeoutMs() {
			return timeoutMs > 0 ? timeoutMs : TIMEOUT_MS_DEFAULT; 
		}
		public long warnMs() {
			return warnMs > 0 ? warnMs : Long.MAX_VALUE; 
		}
		public int maxPoolSize() {
			return maxPoolSize;
		}
		public int corePoolSize() {
			return corePoolSize ;
		}
		public int queueSize() {
			return queueSize;
		}
		public AsyncNameService build() {
			return new AsyncNameService(this);
		}
	}
}
