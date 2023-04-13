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
package com.ms.silverking.cloud.dht;

import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * <p>For a given DHT, this class defines when automatic namespace
 * creation is allowed and what options will be used when it is used.</p>
 *
 * <p>Explicit creation is when a user explicitly calls createNamespace.
 * Auto creation is where a user does *not* call createNamespace, but - rather - the
 * namespace is created when a put is first issued.</p>
 *
 * <p>This class should not be created programmatically by applications. Rather,
 * this class is defined administratively when defining a DHT, and applications
 * may obtain an instance through DHTSession.</p>
 */
public class NamespaceCreationOptions {
  private final Mode mode;
  private final String regex;
  private final NamespaceOptions defaultNSOptions;

  /**
   * Controls when namespaces may be automatically created versus explicitly created.
   */
  public enum Mode {
    /**
     * Users must always explicitly create a namespace before use.
     */
    RequireExplicitCreation,
    /**
     * Users must never explicitly create a namespace. Namespaces will be
     * automatically created using DHT-wide specified NamespaceOptions.
     */
    RequireAutoCreation,
    /**
     * Users may either explicitly create namespaces or
     * optionally autocreate namespaces. Autocreation will only be allowed
     * for namespaces matching the given regular expression.
     */
    OptionalAutoCreation_AllowMatches,
    /**
     * Users may either explicitly create namespaces or
     * optionally autocreate namespaces. Autocreation will only be disallowed
     * for namespaces matching the given regular expression.
     */
    OptionalAutoCreation_DisallowMatches
  }

  ;

  /**
   * Default NamespaceCreationOptions. Subject to change. Recommended practice is for each SilverKing instance
   * to explicitly specify.
   */
  public static final NamespaceCreationOptions defaultOptions = DHTConstants.defaultNamespaceCreationOptions;
  /**
   * Internal use only
   */
  private static final NamespaceCreationOptions emptyTemplate = new NamespaceCreationOptions(null, null, null);

  static {
    ObjectDefParser2.addParser(emptyTemplate);
  }

  /**
   * NamespaceCreationOptions constructor for internal use
   *
   * @param mode             namespace creation mode
   * @param regex            regex for namespace creation modes that require a regex
   * @param defaultNSOptions default NamespaceOptions to use
   */
  public NamespaceCreationOptions(Mode mode, String regex, NamespaceOptions defaultNSOptions) {
    this.mode = mode;
    this.regex = regex;
    this.defaultNSOptions = defaultNSOptions;
  }

  /**
   * Return whether or not the given namespace can be explicitly created given the mode and regex in place
   *
   * @param ns namespace to test
   * @return whether or not the given namespace can be explicitly created given the mode and regex in place
   */
  public boolean canBeExplicitlyCreated(String ns) {
    return mode != Mode.RequireAutoCreation;
  }

  /**
   * Return whether or not the given namespace can be auto created given the mode and regex in place
   *
   * @param ns namespace to test
   * @return whether or not the given namespace can be auto created given the mode and regex in place
   */
  public boolean canBeAutoCreated(String ns) {
    return mode == Mode.RequireAutoCreation || (mode == Mode.OptionalAutoCreation_AllowMatches && ns.matches(
        regex)) || (mode == Mode.OptionalAutoCreation_DisallowMatches && !ns.matches(regex));
  }
    
    /*
    public Mode getMode() {
        return mode;
    }

    public String getRegex() {
        return regex;
    }
    */

  /**
   * Return defaultNSOptions
   *
   * @return defaultNSOptions
   */
  public NamespaceOptions getDefaultNamespaceOptions() {
    return defaultNSOptions;
  }

  /**
   * mode:regex:defaultnsoptions
   */
    /*
    public static NamespaceCreationOptions parse(String def) {
        String[]    defs;
        
        defs = def.split(":");
        if (defs.length != 3) {
            for (int i = 0; i < defs.length; i++) {
                System.err.printf("%d\t%s\n", i, defs[i]);
            }
            throw new RuntimeException("Invalid definition: "+ def);
        } else {
            Mode    mode;
            String  regex;
            NamespaceOptions    defaultNSOptions;
            
            mode = Mode.valueOf(defs[0]);
            regex = defs[1];
            defaultNSOptions = NamespaceOptions.parse(defs[2]);
            return new NamespaceCreationOptions(mode, regex, defaultNSOptions); 
        }
    }
    
    @Override
    public String toString() {
        return mode +":"+ regex +":"+ defaultNSOptions;
    }
    */

  /**
   * Parse a NamespaceCreationOptions definition
   *
   * @param def a NamespaceCreationOptions definition in SilverKing ObjectDefParser format
   * @return a parsed NamespaceCreationOptions instance
   */
  public static NamespaceCreationOptions parse(String def) {
    return ObjectDefParser2.parse(NamespaceCreationOptions.class, def);
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }
    
    /*
     * for unit testing only
    public static void main(String[] args) {
        for (String arg : args) {
        //    System.out.printf("%s\t%s\n", defaultOptions.canBeAutoCreated(ns), defaultOptions
        * .canBeExplicitlyCreated(arg));
            System.out.println(parse(arg));
        }
    }
    */
}
