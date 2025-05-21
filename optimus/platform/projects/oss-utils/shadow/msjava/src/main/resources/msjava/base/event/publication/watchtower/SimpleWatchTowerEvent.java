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
package msjava.base.event.publication.watchtower;
import java.util.Map;
import java.util.Objects;
import msjava.base.annotation.Experimental;
import msjava.base.util.uuid.MSUuid;
@Experimental
public class SimpleWatchTowerEvent extends WatchTowerEvent {
	private final String uuid;
	private final Map<String, String> payload;
	private final String resource; 
	private final String type;
	private final String instanceName;
	private final long creationTime = System.currentTimeMillis();
	
	@Deprecated
	public SimpleWatchTowerEvent(String uuid, 
			String resource, String type,
			Map<String, String> payload, String instanceName) {
		this.uuid = uuid == null ? MSUuid.generateJavaUUID().toString() : uuid;
		Objects.requireNonNull(payload);
		Objects.requireNonNull(type);
		this.payload = payload;
		this.resource = resource;
		this.type = type;
		this.instanceName = instanceName;
	}
	
	public SimpleWatchTowerEvent(String uuid, 
			String type,
			Map<String, String> payload, String instanceName) {
		this.uuid = uuid == null ? MSUuid.generateJavaUUID().toString() : uuid;
		Objects.requireNonNull(payload);
		Objects.requireNonNull(type);
		this.payload = payload;
		this.resource = null;
		this.type = type;
		this.instanceName = instanceName;
	}
	
	@Deprecated
	public SimpleWatchTowerEvent(String resource, String type, Map<String, String> payload) {
		this(null, resource, type, payload, null);
	}
	
	public SimpleWatchTowerEvent(String type, Map<String, String> payload) {
		this(null, type, payload, null);
	}
	@Override
	public String uuid() {
		return uuid;
	}
	@Override
	public String payload() {
		return toString();
	}
	
	@Override
	public Map<String, String> payloadMap() {
		return payload;
	}
	@Deprecated
	@Override
	public String resource() {
		return resource;
	}
	@Override
	public String type() {
		return type;
	}
	@Override
	public long creationTime() {
		return creationTime;
	}
	@Override
	public String instanceName() {
		return instanceName;
	}
	
	@Override
	public String toString() {
		return "SimpleWatchTowerEvent [uuid=" + uuid + ", payload=" + payload
				+ ", resource=" + resource + ", type=" + type
				+ ", instanceName=" + instanceName + ", creationTime="
				+ creationTime + "]";
	}
	
}
