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

public class ExclusionSet extends ServerSetExtension {
  private ExclusionSet(ServerSet serverSet, long mzxid) {
    super(serverSet);
    this.mzxid = mzxid;
  }

  public ExclusionSet(ServerSet serverSet) {
    this(serverSet, INVALID_ZXID);
  }

  private ExclusionSet(long version) {
    this(new ServerSet(new HashSet<>(), version));
  }

  public ExclusionSet(Set<String> excludedEntities, long version, long mzxid) {
    this(new ServerSet(excludedEntities, version), mzxid);
  }

  public static ExclusionSet emptyExclusionSet(long version) {
    return new ExclusionSet(version);
  }

  @Override
  public ExclusionSet addByIPAndPort(Set<IPAndPort> newExcludedEntities) {
    return (ExclusionSet) super.addByIPAndPort(newExcludedEntities);
  }

  @Override
  public ExclusionSet add(Set<String> newExcludedEntities) {
    return new ExclusionSet(serverSet.add(newExcludedEntities));
  }

  @Override
  public ExclusionSet removeByIPAndPort(Set<IPAndPort> newExcludedEntities) {
    return (ExclusionSet) super.removeByIPAndPort(newExcludedEntities);
  }

  @Override
  public ExclusionSet remove(Set<String> newExcludedEntities) {
    return new ExclusionSet(serverSet.remove(newExcludedEntities));
  }

  public static ExclusionSet parse(String def) {
    return new ExclusionSet(
        new ServerSet(CollectionUtil.parseSet(def, singleLineDelimiter), VersionedDefinition.NO_VERSION));
  }

  public static ExclusionSet parse(File file) throws IOException {
    return new ExclusionSet(ServerSet.parse(new FileInputStream(file), VersionedDefinition.NO_VERSION));
  }

  public static ExclusionSet union(ExclusionSet s1, ExclusionSet s2) {
    ExclusionSet u;

    u = emptyExclusionSet(0);
    u = u.add(s1.getServers());
    u = u.add(s2.getServers());
    return u;
  }
  
  public static ExclusionSet difference(ExclusionSet s1, ExclusionSet s2) {
    Set<String> s;

    s = new HashSet<>(s1.getServers());
    s.removeAll(s2.getServers());
    return new ExclusionSet(s, 0, INVALID_ZXID);
  }
}
