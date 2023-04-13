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
package com.ms.silverking.cloud.dht.client.impl;

public class PutMessageEstimate extends KeyedMessageEstimate {
  private int numBytes;
  //public final RuntimeException  re;

  public PutMessageEstimate(int numKeys, int numBytes) {
    super(numKeys);
    this.numBytes = numBytes;
        /*
        try {
            throw new RuntimeException();
        } catch (RuntimeException re) {
            this.re = re;
        }
        */
  }

  public PutMessageEstimate() {
    this(0, 0);
  }

  public void addBytes(int delta) {
    numBytes += delta;
    //sb.append(" +b:"+ delta);
  }

  public void add(PutMessageEstimate oEstimate) {
    super.add(oEstimate);
    numBytes += oEstimate.numBytes;
  }

  public int getNumBytes() {
    return numBytes;
  }

  @Override
  public String toString() {
    return super.toString() + ":" + numBytes;// +"\t"+ sb.toString();
  }
}
