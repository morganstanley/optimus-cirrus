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
package optimus.examples.platform03.relational;

public class PositionImpl implements IPosition {
  private String portfolio;
  private String symbol;
  private Integer quantity;

  public PositionImpl() {}

  @Override
  public String toString() {
    return "Position [portfolio="
        + portfolio
        + ", symbol="
        + symbol
        + ", quantity="
        + quantity
        + "]";
  }

  public PositionImpl(String portfolio, String symbol, Integer quantity) {
    this.portfolio = portfolio;
    this.symbol = symbol;
    this.quantity = quantity;
  }

  public String getPortfolio() {
    return portfolio;
  }

  public void setPortfolio(String portfolio) {
    this.portfolio = portfolio;
  }

  public String getSymbol() {
    return symbol;
  }

  public void setSymbol(String symbol) {
    this.symbol = symbol;
  }

  public Integer getQuantity() {
    return quantity;
  }

  public void setQuantity(Integer quantity) {
    this.quantity = quantity;
  }

  public static PositionImpl of(String portfolio, String symbol, Integer quantity) {
    return new PositionImpl(portfolio, symbol, quantity);
  }
}
