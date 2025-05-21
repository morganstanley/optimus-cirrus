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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import msjava.base.annotation.Experimental;
import msjava.base.event.publication.EventPublisher.Listener;
@Experimental
public abstract class Event {
	
	
	public abstract String uuid();
	
	
	public abstract String payload();
	
	
	public abstract long creationTime();
	
	
	public final boolean isSuccessfullyPublished() {
		return successfullyPublished;
	}
	
	
	public final Rejection publicationRejection() {
		return rejection;
	}
	
	
	public final boolean waitForPublication(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
		if (completionLatch.await(timeout, unit)) {
			return isSuccessfullyPublished();
		}
		else {
			throw new TimeoutException();
		}
	}
	
	
	public final boolean waitForPublication() throws InterruptedException {
		completionLatch.await();
		return isSuccessfullyPublished();
	}
	
	
	
	
	
	
	
	
	
	
	RejectionImpl rejection = null;
	boolean successfullyPublished = false;
	Listener listener = null;
	CountDownLatch completionLatch = new CountDownLatch(1);
}