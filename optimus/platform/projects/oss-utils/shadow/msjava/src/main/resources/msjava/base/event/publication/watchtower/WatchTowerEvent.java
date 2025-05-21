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
import msjava.base.annotation.Experimental;
import msjava.base.event.publication.Event;
import msjava.base.event.publication.PayloadMapEvent;
@Experimental
public abstract class WatchTowerEvent extends Event implements PayloadMapEvent {
	
	@Deprecated
	public String resource() {
		return null;
	}
	public abstract Map<String, String> payloadMap();
	
	
	public abstract String type();
	public abstract String instanceName();
	
}
