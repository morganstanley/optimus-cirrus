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
package com.ms.silverking.cloud.storagepolicy;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.ms.silverking.cloud.meta.VersionedDefinition;
import com.ms.silverking.io.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolicyParser {
  public PolicyParser() {}

  private static Logger log = LoggerFactory.getLogger(PolicyParser.class);

  private static final char OPEN_BRACE = '{';
  private static final char CLOSE_BRACE = '}';
  private static final String rootLabelStart = "StoragePolicyGroup";
  private static final String rootToken = "root";

  public StoragePolicyGroup parsePolicyGroup(File policyFile, long version)
      throws PolicyParseException {
    try {
      return parsePolicyGroup(FileUtil.readFileAsString(policyFile), version);
    } catch (IOException ioe) {
      throw new PolicyParseException(ioe);
    }
  }

  public StoragePolicyGroup parsePolicyGroup(String def, long version) throws PolicyParseException {
    List<LabeledBlock> labeledBlocks;
    List<StoragePolicy> storagePolicies;
    LabeledBlock rootBlock;
    NodeClassAndName rootClassAndName;
    String policyGroupName;

    labeledBlocks = this.parseMultipleLabeledBlocks(def);
    rootBlock = removeBlock(labeledBlocks, rootLabelStart);
    policyGroupName = parsePolicyGroupName(rootBlock.label);
    rootClassAndName = parseRootClassAndName(rootBlock.block);
    storagePolicies = new ArrayList<>();
    for (LabeledBlock labeledBlock : labeledBlocks) {
      storagePolicies.add(parsePolicy(labeledBlock));
    }
    return StoragePolicyGroup.create(policyGroupName, rootClassAndName, storagePolicies, version);
  }

  private String parsePolicyGroupName(String label) throws PolicyParseException {
    String[] tokens;

    tokens = label.split(":");
    if (tokens.length != 2) {
      throw new PolicyParseException("bad policy group label");
    } else if (!tokens[0].equals(rootLabelStart)) {
      throw new RuntimeException("panic");
    } else {
      return tokens[1];
    }
  }

  private static NodeClassAndName parseRootClassAndName(String def) throws PolicyParseException {
    String[] tokens;

    tokens = def.split("\\s+");
    if (tokens.length != 2) {
      throw new PolicyParseException("bad root def");
    } else if (!tokens[0].equals(rootToken)) {
      throw new PolicyParseException("bad root token");
    } else {
      return NodeClassAndName.parse(tokens[1]);
    }
  }

  private static LabeledBlock removeBlock(List<LabeledBlock> labeledBlocks, String labelStart) {
    for (int i = 0; i < labeledBlocks.size(); i++) {
      if (labeledBlocks.get(i).getLabel().startsWith(labelStart)) {
        return labeledBlocks.remove(i);
      }
    }
    throw new RuntimeException("Couldn't find block for removal: " + labelStart);
  }

  public StoragePolicy parsePolicy(LabeledBlock policyBlock) throws PolicyParseException {
    // LabeledBlock    policyBlock;
    List<SubPolicy> subPolicies;
    SubPolicy primarySubPolicy;
    SubPolicy secondarySubPolicy;

    // policyBlock = parseLabeledBlock(def);

    subPolicies = new ArrayList<>();
    for (LabeledBlock subPolicyBlock : parseMultipleLabeledBlocks(policyBlock.block)) {
      subPolicies.add(parseSubPolicy(subPolicyBlock));
    }
    primarySubPolicy = findSubPolicy(subPolicies, ReplicationType.Primary);
    if (primarySubPolicy == null) {
      throw new PolicyParseException("No primary policy specified");
    }
    secondarySubPolicy = findSubPolicy(subPolicies, ReplicationType.Secondary);
    return new StoragePolicy(
        NodeClassAndName.parse(policyBlock.label), primarySubPolicy, secondarySubPolicy);
  }

  private SubPolicy findSubPolicy(List<SubPolicy> subPolicies, ReplicationType type) {
    for (SubPolicy subPolicy : subPolicies) {
      if (subPolicy.getReplicationType() == type) {
        return subPolicy;
      }
    }
    return null;
  }

  private SubPolicy parseSubPolicy(LabeledBlock block) throws PolicyParseException {
    return new SubPolicy(
        parseReplicationType(block.getLabel()), SubPolicyMember.parseMultiple(block.getBlock()));
  }

  private ReplicationType parseReplicationType(String def) throws PolicyParseException {
    for (ReplicationType rType : ReplicationType.values()) {
      if (rType.toString().equalsIgnoreCase(def)) {
        return rType;
      }
    }
    throw new PolicyParseException("Unknown ReplicationType: " + def);
  }

  private List<LabeledBlock> parseMultipleLabeledBlocks(String def) throws PolicyParseException {
    List<LabeledBlock> blocks;
    String[] defAndBlock;

    blocks = new ArrayList<>();
    defAndBlock = new String[2];
    while (def != null && def.length() > 0) {
      def = def.trim();
      if (def.length() > 0) {
        String block;

        defAndBlock[0] = def;
        removeNextBlock(defAndBlock);
        // System.out.println(defAndBlock[0] +"\t::\t"+ defAndBlock[1]);
        def = defAndBlock[0];
        block = defAndBlock[1];
        blocks.add(parseLabeledBlock(def));
        def = block;
      }
    }
    return blocks;
  }

  private void removeNextBlock(String[] defAndBlock) throws PolicyParseException {
    String def;
    int i;
    int depth;

    def = defAndBlock[0];
    i = def.indexOf(OPEN_BRACE);
    depth = 1;
    while (depth > 0) {
      i++;
      if (i >= def.length()) {
        throw new PolicyParseException("Bad multiple block def: " + def);
      }
      switch (def.charAt(i)) {
        case OPEN_BRACE:
          depth++;
          break;
        case CLOSE_BRACE:
          depth--;
          break;
      }
    }
    defAndBlock[0] = def.substring(0, i + 1).trim();
    if (i < def.length()) {
      defAndBlock[1] = def.substring(i + 1).trim();
    } else {
      defAndBlock[1] = null;
    }
  }

  private LabeledBlock parseLabeledBlock(String def) throws PolicyParseException {
    int b0;
    int b1;
    String label;
    String block;

    b0 = getIndex(OPEN_BRACE, def, IndexType.first);
    b1 = getIndex(CLOSE_BRACE, def, IndexType.last);
    label = def.substring(0, b0).trim();
    block = def.substring(b0 + 1, b1).trim();
    return new LabeledBlock(label, block);
  }

  private enum IndexType {
    first,
    last
  };

  private int getIndex(char token, String s, IndexType indexType) throws PolicyParseException {
    int i;

    if (indexType == IndexType.first) {
      i = s.indexOf(token);
    } else {
      i = s.lastIndexOf(token);
    }
    if (i < 0) {
      throw new PolicyParseException("Can't find " + token + " in " + s);
    }
    // System.out.println(token +" "+ s +" "+ indexType +" "+ i);
    return i;
  }

  private static class LabeledBlock {
    private final String label;
    private final String block;

    LabeledBlock(String label, String block) {
      this.label = label;
      this.block = block;
    }

    String getLabel() {
      return label;
    }

    String getBlock() {
      return block;
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      if (args.length == 0) {
        log.debug("args: <policyFile>");
      } else {
        File policyFile;
        StoragePolicyGroup storagePolicyGroup;

        policyFile = new File(args[0]);
        storagePolicyGroup =
            new PolicyParser().parsePolicyGroup(policyFile, VersionedDefinition.NO_VERSION);
        log.info("{}", storagePolicyGroup);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
