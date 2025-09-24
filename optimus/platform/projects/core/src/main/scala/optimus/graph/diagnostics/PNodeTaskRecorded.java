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
package optimus.graph.diagnostics;

import optimus.graph.NodeTask;
import optimus.graph.NodeTaskInfo;
import optimus.graph.PThreadContext;

public class PNodeTaskRecorded extends PNodeTask {
  public int id; // ID (same as NodeTask.getID, but obtained from the event trace)
  public PNodeTaskInfo info = fakeInfo; // Default to avoid null checks, but expected to be updated!

  public static class Values {
    public int thisHash;
    public int resultHash;
    public int argHash;
    public Object result;
  }

  public Values values;

  public PNodeTaskRecorded(int tid) {
    id = tid;
  }

  @Override
  public int id() {
    return id;
  }

  @Override
  public PThreadContext.Process process() {
    return info.getProcess();
  }

  @Override
  public int infoId() {
    return info.id;
  }

  // REVIEW TO SEE IF IT CORRECTLY RECORDS THE PROFILE
  @Override
  public Object subProfile() {
    return null;
  }

  @Override
  public NodeTask getTask() {
    return fakeTask;
  }

  @Override
  public boolean isLive() {
    return false;
  }

  @Override
  public String stateAsString() {
    return "REC";
  }

  @Override
  public String stateAsDetailedString(boolean html) {
    return "(Recorded node; no further details)";
  }

  @Override
  public NodeName nodeName() {
    return info.nodeName();
  }

  @Override
  public String toPrettyName(boolean entityType, boolean simpleName, boolean includeHint) {
    return nodeName().toString(simpleName, includeHint);
  }

  @Override
  public String propertyFlagsAsString() {
    return info.flagsAsString();
  }

  @Override
  public String propertyFlagsAsDetailedString(boolean html) {
    String str = info.flagsAsStringVerbose(html);
    return html ? "<html>" + str + "</html>" : str;
  }

  @Override
  public String propertyCachePolicy() {
    return info.cachePolicy;
  }

  @Override
  public boolean isScenarioIndependent() {
    return info.isScenarioIndependent();
  }

  @Override
  public boolean isInternal() {
    return id == 0 || info.isInternal();
  }

  @Override
  public boolean isCacheable() {
    return info.getCacheable();
  }

  @Override
  public boolean isProxy() {
    return info.isProfilerProxy();
  }

  @Override
  public boolean isTweakNode() {
    return NodeTaskInfo.NAME_MODIFIER_TWEAK.equals(info.modifier);
  }

  @Override
  public boolean isUITrack() {
    return NodeTaskInfo.UITrack.name().equals(info.name);
  }

  @Override
  public Object[] args() {
    return values != null ? new Object[] {values.argHash} : null;
  }

  @Override
  public String resultAsString() {
    if (values == null) return "Not collected";
    return (values.result == null)
        ? Integer.toHexString(values.resultHash)
        : values.result.toString();
  }

  @Override
  public String resultDisplayString() {
    return resultAsString();
  }

  @Override
  public Object resultKey() {
    return values != null ? values.resultHash : "Not collected";
  }

  @Override
  public Object methodThisKey() {
    return values != null ? values.thisHash : "Not collected";
  }
}
