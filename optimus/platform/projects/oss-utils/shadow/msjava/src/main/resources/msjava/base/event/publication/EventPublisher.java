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
import java.util.Map;
import msjava.base.annotation.Experimental;
import msjava.base.event.publication.endpoint.PublicationEndpoint;
import msjava.base.event.publication.registry.EventPublisherBuilder;
import msjava.base.event.publication.registry.EventPublisherRegistry;
@Experimental
public interface EventPublisher {
	
	
	void publishAsync(Event event);
	
	
	void publishAsync(Event event, Listener listener);
	
	
	void publish(Event e) throws EventPublicationException, InterruptedException;
	
	
	Event publishAsync(String eventType, Map<String, String> payload);
	
	
	
	Event publishAsync(String eventType, Map<String, String> payload, Listener listener);
	
	void publish(String eventType, Map<String, String> payload) throws InterruptedException;
	
	
	
	public static interface Listener {
		
		
		void onSuccessfulPublication(Event e);
		
		
		void onFailedPublication(Rejection r);
		
		
		void onRetryPublication(Rejection r);
	}
	
	
	public static class ListenerAdapter implements Listener {
		@Override
		public void onSuccessfulPublication(Event e) {}
		@Override
		public void onFailedPublication(Rejection r) {}
		@Override
		public void onRetryPublication(Rejection r) {}
		
	}
	
	
	
	@SuppressWarnings("serial")
	public static class EventPublicationException extends RuntimeException {
		private final Rejection rejection;
		
		public EventPublicationException(Rejection rejection) {
			super(rejection.cause());
			this.rejection = rejection;
			
		}
		public EventPublicationException(String message, Rejection rejection) {
			super(message, rejection.cause());
			this.rejection = rejection;
		}
		
		public EventPublicationException(String message) {
			super(message);
			rejection = null;
		}
		
		public Rejection rejection() {
			return rejection;
		}
	}
}