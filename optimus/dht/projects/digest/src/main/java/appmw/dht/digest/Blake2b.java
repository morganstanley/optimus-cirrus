/*
 * Blake2b implementation is based on excellent work at https://github.com/alphazero/Blake2b, which
 * was placed in public domain worldwide.
 *
 * Blake2bSafe is based on the default branch and this commit (23th August 2018):
 * https://github.com/alphazero/Blake2b/commit/59d2c8a0efc0f4708ce2cc3b8e049f1b97918bd0
 * Blake2bUnsafe is based on unsafe branch and this commit (23th August 2018):
 * https://github.com/alphazero/Blake2b/commit/ea2e468b511217513e1bcb143375c7154921931a
 *
 * The only changes from the original are package and interface names (this file) and javadoc error
 * fixes. For those changes only:
 *
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
package appmw.dht.digest;

import appmw.dht.digest.Blake2bUnsafe.Digest;

public class Blake2b {

  private static final ThreadLocal<Digest> digest256TL =
      new ThreadLocal<Digest>() {
        @Override
        protected Digest initialValue() {
          return Blake2bUnsafe.Digest.newInstance(32);
        }
      };

  private static final ThreadLocal<Digest> digest512TL =
      new ThreadLocal<Digest>() {
        @Override
        protected Digest initialValue() {
          return Blake2bUnsafe.Digest.newInstance(64);
        }
      };

  public static byte[] digest256(byte[] input) {
    return digest256TL.get().digest(input);
  }

  public static byte[] digest512(byte[] input) {
    return digest512TL.get().digest(input);
  }
}
