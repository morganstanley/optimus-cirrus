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
package com.ms.silverking.cloud.toporing.meta;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ms.silverking.cloud.meta.VersionedDefinition;
import com.ms.silverking.cloud.topology.Node;

public class WeightSpecifications implements VersionedDefinition {
  private final long version;
  //private final Map<String, Double>   classWeights;
  private final Map<String, Double> nodeWeights;

  // TODO (OPTIMUS-0000): we need to be able to specify weights by groups of some sort
  // start with host groups, and require other topology levels to specify weights by ID

  public static final double defaultWeight = 1.0;

  public WeightSpecifications(long version) {
    this.version = version;
    //classWeights = new HashMap<>();
    nodeWeights = new HashMap<>();
  }

  public WeightSpecifications(long version, Map<String, Double> nodeWeights) {
    this.version = version;
    this.nodeWeights = new HashMap<>(nodeWeights);
  }

  //public void setClassWeight(NodeClass nodeClass, double weight) {
  //    classWeights.put(nodeClass.g, weight);
  //}

  private void setNodeWeightByID(String nodeName, double weight) {
    nodeWeights.put(nodeName, weight);
  }
    
    /*
    public double getWeight(NodeClass nodeClass) {
        Double  weight;
        
        weight = classWeights.get(nodeClass.getName());
        if (weight == null) {
            return 0.0;
        } else {
            return weight;
        }
    }
    */

  public double getWeight(Node node) {
    Double weight;

    weight = nodeWeights.get(node.getIDString());
    if (weight != null) {
      return weight;
    } else {
            /*
            weight = getWeight(node.getNodeClass());
            if (weight == 0.0) {
                return defaultWeight;
            } else {
                return weight;
            }
            */
      return defaultWeight;
    }
  }

  public List<Double> getWeights(List<Node> nodes) {
    List<Double> weights;

    weights = new ArrayList<>(nodes.size());
    for (Node node : nodes) {
      weights.add(getWeight(node));
    }
    return weights;
  }

  public WeightSpecifications parse(File file) throws IOException {
    return parse(new FileInputStream(file));
  }

  public WeightSpecifications parse(InputStream inStream) throws IOException {
    BufferedReader reader;
    String line;

    reader = new BufferedReader(new InputStreamReader(inStream));
    do {
      line = reader.readLine();
      if (line != null) {
        line = line.trim();
        if (line.length() > 0) {
          String[] toks;

          toks = line.split("\\s+");
          if (toks.length >= 2) {
            setNodeWeightByID(toks[0], Double.parseDouble(toks[1]));
          }
        }
      }
    } while (line != null);
    return this;
  }

  public Collection<Map.Entry<String, Double>> getNodeWeights() {
    return nodeWeights.entrySet();
  }

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    for (Map.Entry<String, Double> entry : nodeWeights.entrySet()) {
      sb.append(entry.getKey());
      sb.append('\t');
      sb.append(entry.getValue());
      sb.append('\n');
    }
    return sb.toString();
  }

  @Override
  public long getVersion() {
    return version;
  }
}
