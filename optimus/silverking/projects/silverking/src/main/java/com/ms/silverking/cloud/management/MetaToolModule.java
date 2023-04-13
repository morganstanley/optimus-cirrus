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
package com.ms.silverking.cloud.management;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;

public interface MetaToolModule<T> {
  public T readFromFile(File file, long version) throws IOException;

  public T readFromZK(long version, MetaToolOptions options) throws KeeperException;

  public void writeToFile(File file, T instance) throws IOException;

  /**
   * Write to zookeeper using the version provided and ignoring any
   * version already present.
   *
   * @param options TODO
   * @return TODO
   */
  public String writeToZK(T instance, MetaToolOptions options) throws IOException, KeeperException;

  public void deleteFromZK(long version) throws KeeperException, ExecutionException, InterruptedException;

  public long getLatestVersion() throws KeeperException;
}
