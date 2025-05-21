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
package msjava.base.event.publication;
import msjava.base.annotation.Internal;
import com.google.common.base.Throwables;
@Internal
public class RejectionImpl implements Rejection {
	Event event;
	Throwable cause;
	Source source;
	int retryCount = 0;
	
	public RejectionImpl(Event event, Throwable cause, Source source) {
		this.event = event;
		this.cause = cause;
		this.source = source;
	}
	@Override
	public Event event() {
		return event;
	}
	@Override
	public Throwable cause() {
		return cause;
	}
	@Override
	public Source source() {
		return source;
	}
	@Override
	public int retryCount() {
		return retryCount;
	}
	@Override
	public String toString() {
		return "RejectionImpl [event=" + event + ", cause=" + Throwables.getStackTraceAsString(cause)
				+ ", source=" + source + ", retryCount=" + retryCount + "]";
	}
}