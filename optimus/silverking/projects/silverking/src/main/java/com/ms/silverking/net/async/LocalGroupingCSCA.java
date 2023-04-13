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

import java.net.InetSocketAddress;
import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.net.InetAddressUtil;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * Reserve a single SelectorController for all local connections. Spread others over selector controllers
 * grouped by address.
 */
public class LocalGroupingCSCA<T extends Connection> implements ChannelSelectorControllerAssigner<T> {
  private final int numLocalSelectors;
  private final AtomicInteger nextLocalIndex;
  private final AtomicInteger nextNonLocalIndex;

  private static Logger log = LoggerFactory.getLogger(LocalGroupingCSCA.class);

  private static final int defaultNumLocalSelectors = 1;

  public LocalGroupingCSCA(int numLocalSelectors) {
    if (numLocalSelectors < 0) {
      throw new RuntimeException("numLocalSelectors < 0");
    }
    this.numLocalSelectors = numLocalSelectors;
    nextLocalIndex = new AtomicInteger();
    nextNonLocalIndex = new AtomicInteger();
  }

  public LocalGroupingCSCA() {
    this(defaultNumLocalSelectors);
  }

  @Override
  /**
   * Assigns, the given channel to a SelectorController chosen from the list.
   * Does *not* add the channel to the SelectorController.
   */ public SelectorController<T> assignChannelToSelectorController(SelectableChannel channel,
                                                                     List<SelectorController<T>> selectorControllers) {
    boolean isLocal;
    int index;

    if (selectorControllers.size() <= numLocalSelectors) {
      throw new RuntimeException("selectorControllers.size() <= numLocalSelectors");
    }
    if (numLocalSelectors == 0) {
      isLocal = false;
    } else {
      if (channel instanceof ServerSocketChannel) {
        //index = localIndex;
        isLocal = true;
      } else {
        SocketChannel socketChannel;
        InetSocketAddress socketAddress;

        socketChannel = (SocketChannel) channel;
        socketAddress = (InetSocketAddress) socketChannel.socket().getRemoteSocketAddress();
        if (InetAddressUtil.isLocalHostIP(socketAddress.getAddress())) {
          //index = localIndex;
          isLocal = true;
        } else {
          //index = socketAddress.hashCode() % (selectorControllers.size() - numLocalSelectors) + numLocalSelectors;
          isLocal = false;
        }
      }
    }
    if (isLocal) {
      index = nextLocalIndex.getAndIncrement() % numLocalSelectors;
    } else {
      int numNonLocalSelectorControllers;

      numNonLocalSelectorControllers = selectorControllers.size() - numLocalSelectors;
      assert numNonLocalSelectorControllers > 0;
      index = numLocalSelectors + (nextNonLocalIndex.getAndIncrement() % numNonLocalSelectorControllers);
    }
    return selectorControllers.get(index);
  }
}
