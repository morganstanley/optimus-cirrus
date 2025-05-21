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
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import msjava.base.slf4j.ContextLogger;
import msjava.base.util.internal.SystemPropertyUtils;
public class CachingNameService implements MSNameService {
	
	private static final Logger logger = ContextLogger.safeLogger();
	
	public static final String TTL_SEC_PROPERTY = "msjava.base.dns.cache.ttl_sec";
	public static final long TTL_SEC_DEFAULT = 30;
	public static final String MAX_SIZE_PROPERTY = "msjava.base.dns.cache.max_size";
	public static final int MAX_SIZE_DEFAULT = 1000;
	public static final String NEGATIVE_ENABLED_PROPERTY = "msjava.base.dns.cache.negative.enabled";
	public static final boolean NEGATIVE_ENABLED_DEFAULT = true;
	
	public static final String ENABLED_PROPERTY = "msjava.base.dns.cache.enabled";
	public static final boolean ENABLED_DEFAULT = true;
	
	private final MSNameService delegate;
	private final boolean negativeEnabled;
	
	private final LoadingCache<ByteArrayKey, HostnameOrException> cache;
	
	public CachingNameService() {
		this(new Builder());
	}
	
	private CachingNameService(Builder builder) {
		this.delegate = builder.delegate();
		this.negativeEnabled = builder.negativeEnabled();
		
		this.cache = CacheBuilder.newBuilder()
				.expireAfterWrite(builder.ttlSec(), TimeUnit.SECONDS)
				.maximumSize(builder.maxSize())
				.build(new CacheLoader<ByteArrayKey, HostnameOrException>() {
					@Override
					public HostnameOrException load(ByteArrayKey key) throws UnknownHostException {
						return getHostByAddrForCache(key);
					}
				});
	}
	@Override
	public InetAddress[] lookupAllHostAddr(String hostname) throws UnknownHostException {
		return delegate.lookupAllHostAddr(hostname); 
	}
	@Override
	public String getHostByAddr(byte[] addr) throws UnknownHostException {
		
		ByteArrayKey key = new ByteArrayKey(addr);
		try {
			logger.trace("Looking up {} in cache", key);
			HostnameOrException hostname = cache.get(key);
			return hostname.orElseThrow(() -> new UnknownHostException("Unknown host " + key + " (cached)"));
		} 
		catch (ExecutionException e) {
			if (e.getCause() instanceof UnknownHostException) {
				UnknownHostException uhe = new UnknownHostException(e.getCause().getLocalizedMessage());
				uhe.initCause(e);
				throw uhe;
			}
			throw new RuntimeException("Unexpected exception thrown during DNS reverse lookup of " + key, e);
		}
	}
	@VisibleForTesting
	MSNameService getDelegate() {
		return delegate;
	}
	
	private HostnameOrException getHostByAddrForCache(ByteArrayKey key) throws UnknownHostException {
		try {
			logger.trace("Cache miss. Looking up {}", key);
			String hostname = delegate.getHostByAddr(key.get());
			return new HostnameOrException(hostname);
		} 
		catch (UnknownHostException e) {
			if (negativeEnabled) {
				return new HostnameOrException(e);
			} else {
				throw e;
			}
		}
	}
	
	
	public static class Builder {
		private MSNameService delegate;
		private long ttlSec = SystemPropertyUtils.getLong(TTL_SEC_PROPERTY, TTL_SEC_DEFAULT, logger);
		private int maxSize = SystemPropertyUtils.getInteger(MAX_SIZE_PROPERTY, MAX_SIZE_DEFAULT, logger);
		private boolean negativeEnabled = SystemPropertyUtils.getBoolean(NEGATIVE_ENABLED_PROPERTY, NEGATIVE_ENABLED_DEFAULT, logger);
		private boolean enabled = SystemPropertyUtils.getBoolean(ENABLED_PROPERTY, ENABLED_DEFAULT, logger);
		
		public Builder delegate(MSNameService delegate) {
			this.delegate = delegate;
			return this;
		}
		
		public Builder ttlSec(long ttlSec) {
			this.ttlSec = ttlSec;
			return this;
		}
		
		public Builder maxSize(int maxSize) {
			this.maxSize = maxSize;
			return this;
		}
		
		public Builder negativeEnabled(boolean negativeEnabled) {
			this.negativeEnabled = negativeEnabled;
			return this;
		}
		
		public Builder enabled(boolean enabled) {
			this.enabled = enabled;
			return this;
		}
		public MSNameService delegate() {
			return delegate == null ? new JDKNameServiceWrapper() : delegate;
		}
		public int maxSize() {
			return maxSize;
		}
		
		public long ttlSec() {
			return ttlSec;
		}
		
		public boolean negativeEnabled() {
			return negativeEnabled;
		}
		
		public boolean enabled() {
			return enabled;
		}
		public MSNameService build() {
			return enabled ? new CachingNameService(this) : delegate();
		}
	}
	
	
	private static class ByteArrayKey {
		private final byte[] key;
		private final int hashcode;
		
		public ByteArrayKey(byte[] key) {
			this.key = Arrays.copyOf(key, key.length);
			this.hashcode = Arrays.hashCode(this.key);
		}
		
		public byte[] get() {
			return Arrays.copyOf(key, key.length);
		}
		@Override
		public int hashCode() {
			return hashcode;
		}
		@Override
		public boolean equals(Object obj) {
	        if (this == obj) {
	            return true;
	        }
	        if (!(obj instanceof ByteArrayKey)) {
	            return false;
	        }
	        ByteArrayKey other = (ByteArrayKey) obj;
	        byte[] otherKey = other.key;
	        return Arrays.equals(otherKey, key);
		}
		
		@Override
		public String toString() {
			if (key == null) return "(null)";
			if (key.length == 0) return "(empty)";
			StringBuilder str = new StringBuilder();
			for (int i = 0; i < key.length; i++) {
				if (i > 0) str.append(".");
				str.append(key[i] & 0xff);
			}
			return str.toString();
		}
	}
	
	
	private static class HostnameOrException {
		private final String hostname;
		private final Exception exception;
		
		public HostnameOrException(String hostname) {
			this.hostname = hostname;
			this.exception = null;
		}
		
		public HostnameOrException(Exception exception) {
			this.hostname = null;
			this.exception = exception;
		}
		
	    public <X extends Throwable> String orElseThrow(Supplier<? extends X> exceptionSupplier) throws X {
	    	if (hostname != null) {
	    		return hostname;
	    	} else {
	    		X toThrow = exceptionSupplier.get();
	    		toThrow.initCause(exception);
	    		throw toThrow;
	    	}
	    }
	}
}
