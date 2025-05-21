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
package msjava.zkapi.leader.optimus;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import msjava.base.annotation.Internal;
import msjava.base.slf4j.ContextLogger;
import msjava.zkapi.ZkaPathContext;
import msjava.zkapi.internal.ZkaContext;
import msjava.zkapi.internal.ZkaData;
import msjava.zkapi.internal.ZkaServiceDescription;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.Lease;
import org.slf4j.Logger;
@Internal
public final class ZkaMutex extends InterProcessMutex implements Closeable {
	private static final Logger log = ContextLogger.safeLogger();
	private final ZkaPathContext context;
    private final byte[] content;
	
	public static final Comparator<String> lockSorter = Comparator.comparing(s -> s.split("-lock-")[1]);
	
	public ZkaMutex(ZkaContext context, ZkaData data) {
	    this(context, endPointUrl(data));
	}
	    
    private ZkaMutex(ZkaPathContext context, byte[] content) {
		super(context.getCurator(), context.paths(""));
		this.content = content;
		this.context = context;
		context.register(this);
	}
    private static byte[] endPointUrl(ZkaData data) {
        Objects.requireNonNull(data, "ZkaData cannot be null");
        String content = Objects.requireNonNull(getContent(data), "ZkaData must contain endPointUrl");
        return content.getBytes(StandardCharsets.UTF_8);
    }
    
	private static String getContent(ZkaData data) {
		return (String) data.get(ZkaServiceDescription.END_POINT_URL);
	}
	@Override
	protected byte[] getLockNodeBytes() {
		return Arrays.copyOf(content, content.length);
	}
	
	public Set<Map<String, Object>> getLeaderData() throws IOException {
	    return candidates(true);
	}
	public Set<Map<String, Object>> getCandidates() throws IOException {
		return candidates(false);
	}
    private Set<Map<String, Object>> candidates(boolean firstOnly) throws IOException {
        Set<Map<String, Object>> data = new HashSet<>();
		try {
			
			if (context.exists("") == null) {
				return data;
			}
			List<String> locks = context.getChildren("");
			if (locks.isEmpty())
				return data;
			Collections.sort(locks, lockSorter);
			for (String lock : locks) {
				data.add(getData(lock));
				if (firstOnly)
				    break;
			}
			return data;
		} catch (IOException | RuntimeException e) {
            log.error("failed to find participating nodes", e);
            throw e;
        } catch (Exception e) {
            log.error("failed to find participating nodes", e);
            throw new IOException(e);
        }
    }
	public Lease acquireLock() throws Exception {
		super.acquire();
		return new Lease() {
			
			@Override
			public byte[] getData() throws Exception {
				return null;
			}
			
			@Override
			public void close() throws IOException {
        		try {
        		    log.info("going to release mutex lock");
        		    ZkaMutex.this.release();
        		    log.info("lock released");
        		} catch (Exception e) {
        			log.warn("Failed to release lock", e);
        		}
			}
            @Override
            public String getNodeName() {
                return null;
            }
		};
	}
	
	private ZkaData getData(String path) throws IOException {
		ZkaData zd = ZkaData.fromBytes(context.getNodeData(path));
		return replaceKeys(zd, "string", ZkaServiceDescription.END_POINT_URL);
	}
	
	private static ZkaData replaceKeys(ZkaData map, String toRemove, String toAdd) {
		Object value = map.remove(toRemove);
		if (value != null)
			map.put(toAdd, value);
		return map;
	}
	
	@Override
	public void close() throws IOException {
		context.unregister(this);
	}
}