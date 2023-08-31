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
package com.ms.silverking.cloud.meta;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.net.IPAndPort;

public class SuspectSet extends ServerSetExtension {
  private SuspectSet(ServerSet serverSet, long mzxid) {
    super(serverSet);
    this.mzxid = mzxid;
  }

  public SuspectSet(ServerSet serverSet) {
    this(serverSet, INVALID_ZXID);
  }

  private SuspectSet(long version) {
    this(new ServerSet(new HashSet<>(), version));
  }

  public SuspectSet(Set<String> excludedEntities, long version, long mzxid) {
    this(new ServerSet(excludedEntities, version), mzxid);
  }

  public static SuspectSet emptySuspectSet(long version) {
    return new SuspectSet(version);
  }

  @Override
  public SuspectSet addByIPAndPort(Set<IPAndPort> newExcludedEntities) {
    return (SuspectSet) super.addByIPAndPort(newExcludedEntities);
  }

  @Override
  public SuspectSet add(Set<String> newExcludedEntities) {
    return new SuspectSet(serverSet.add(newExcludedEntities));
  }

  @Override
  public SuspectSet removeByIPAndPort(Set<IPAndPort> newExcludedEntities) {
    return (SuspectSet) super.removeByIPAndPort(newExcludedEntities);
  }

  @Override
  public SuspectSet remove(Set<String> newExcludedEntities) {
    return new SuspectSet(serverSet.remove(newExcludedEntities));
  }

  public static SuspectSet parse(String def) {
    return new SuspectSet(
        new ServerSet(
            CollectionUtil.parseSet(def, singleLineDelimiter), VersionedDefinition.NO_VERSION));
  }

  public static SuspectSet parse(File file) throws IOException {
    return new SuspectSet(
        ServerSet.parse(new FileInputStream(file), VersionedDefinition.NO_VERSION));
  }

  public static SuspectSet union(SuspectSet s1, SuspectSet s2) {
    SuspectSet u;

    u = emptySuspectSet(0);
    u = u.add(s1.getServers());
    u = u.add(s2.getServers());
    return u;
  }
}
