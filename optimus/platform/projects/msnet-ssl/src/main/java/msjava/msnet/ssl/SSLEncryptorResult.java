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

package msjava.msnet.ssl;

/**
 * This class is returned as a result of calling encrypt/decrypt method on the SSLEncryptor object.
 *
 * <p>Contains statistics about how many bytes have been produced/consumed.
 */
public class SSLEncryptorResult {

  enum State {
    OK,
    BUFFER_UNDERFLOW
  }

  private final int bytesConsumed;
  private final int bytesProduced;
  private final State state;

  static SSLEncryptorResult success(int bytesProduced, int bytesConsumed) {
    return new SSLEncryptorResult(bytesConsumed, bytesProduced, State.OK);
  }

  static SSLEncryptorResult bufferUnderflow(int bytesProduced, int bytesConsumed) {
    return new SSLEncryptorResult(bytesConsumed, bytesProduced, State.BUFFER_UNDERFLOW);
  }

  private SSLEncryptorResult(int bytesConsumed, int bytesProduced, State state) {
    this.bytesConsumed = bytesConsumed;
    this.bytesProduced = bytesProduced;
    this.state = state;
  }

  public int getBytesConsumed() {
    return bytesConsumed;
  }

  public int getBytesProduced() {
    return bytesProduced;
  }

  public State getState() {
    return state;
  }
}
