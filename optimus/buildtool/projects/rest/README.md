[//]: # (Morgan Stanley makes this available to you under the Apache License, Version 2.0 \(the "License"\))
[//]: # (You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.)
[//]: # (See the NOTICE file distributed with this work for additional information regarding copyright ownership.)
[//]: # (Unless required by applicable law or agreed to in writing, software)
[//]: # (distributed under the License is distributed on an "AS IS" BASIS,)
[//]: # (WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.)
[//]: # (See the License for the specific language governing permissions and)
[//]: # (limitations under the License.)
# Rest OBT module

This module exists to implement an OpenAPI builder app for OBT, without forcing OBT to depend on rest_common, which has a lot of transitive dependencies to spring and other unnecessary modules.

The processor is called from OpenApiProcessor at build time and the spec will be output to the install location.

## Example OBT processor declaration

```
  processors {
    myService {
      type = openapi
      template = "com.ms.foo.bar.MyService"
      installLocation = "etc/v3/api_docs/MyService/spec.json"
      configuration.extra_args = "optional extra arg string for MyService"
      configuration.api_path = "optional path to openapi endpoint, if not at /v3/api-docs"
      configuration.timeout = "optional timeout to boot service, default 300s"
    }
  }
```