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
package optimus.excel.input;

import java.util.List;

import scala.Tuple2;

public class InvalidHandleInfo {

  private int paramIndex;
  private Class<?> paramClass;
  private ParamType paramType;

  private String paramValue;
  private List<String> paramValues;

  private List<Integer> arrayIndices;
  private List<Tuple2<Integer, Integer>> matrixIndices;

  public InvalidHandleInfo(
      int paramIndex,
      Class<?> paramClass,
      ParamType paramType,
      List<Integer> arrayIndices,
      List<Tuple2<Integer, Integer>> matrixIndices,
      String paramValue,
      List<String> paramValues) {
    this.paramIndex = paramIndex;
    this.paramClass = paramClass;
    this.paramType = paramType;
    this.arrayIndices = arrayIndices;
    this.matrixIndices = matrixIndices;
    this.paramValue = paramValue;
    this.paramValues = paramValues;
  }

  public int getParamIndex() {
    return paramIndex;
  }

  public Class<?> getParamClass() {
    return paramClass;
  }

  public ParamType getParamType() {
    return paramType;
  }

  public String getParamValue() {
    return paramValue;
  }

  public List<String> getParamValues() {
    return paramValues;
  }

  public List<Integer> getArrayIndices() {
    return arrayIndices;
  }

  public List<Tuple2<Integer, Integer>> getMatrixIndices() {
    return matrixIndices;
  }

  public enum ParamType {
    SCALAR,
    ARRAY,
    MATRIX
  }
}
