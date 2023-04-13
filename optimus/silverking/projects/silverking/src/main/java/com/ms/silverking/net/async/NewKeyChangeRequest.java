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
package com.ms.silverking.net.async;

import java.nio.channels.SelectableChannel;

/**
 * Request sent to Selector
 */
public final class NewKeyChangeRequest extends KeyChangeRequest {
  private final ChannelRegistrationWorker crWorker;

  public NewKeyChangeRequest(SelectableChannel channel, Type type, int newOps, ChannelRegistrationWorker crWorker) {
    super(channel, type, newOps);
    this.crWorker = crWorker;
  }

  public NewKeyChangeRequest(SelectableChannel channel, Type type, ChannelRegistrationWorker crWorker) {
    this(channel, type, 0, crWorker);
  }

  public ChannelRegistrationWorker getChannelRegistrationWorker() {
    return crWorker;
  }

  @Override
  public int hashCode() {
    return super.hashCode() ^ crWorker.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    NewKeyChangeRequest other;

    other = (NewKeyChangeRequest) obj;
    return this.crWorker == other.crWorker && super.equals(other);
  }

  public String toString() {
    return super.toString() + ":" + crWorker;
  }
}
