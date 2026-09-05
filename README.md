# OpenDXL Python Client

[![Latest PyPI Version](https://img.shields.io/pypi/v/dxlclient.svg)](https://pypi.python.org/pypi/dxlclient)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Actions Status](https://github.com/opendxl/opendxl-client-python/workflows/Build/badge.svg)](https://github.com/opendxl/opendxl-client-python/actions)

## Overview

The OpenDXL Python Client enables the development of applications that connect to the [McAfee Data Exchange Layer](http://www.mcafee.com/us/solutions/data-exchange-layer.aspx) messaging fabric for the purposes of sending/receiving events and invoking/providing services.

## Documentation

See the [Wiki](https://github.com/opendxl/opendxl-client-python/wiki) for an overview of the Data Exchange Layer (DXL), the OpenDXL Python client, and examples.

See the [Python Client SDK Documentation](https://opendxl.github.io/opendxl-client-python/pydoc) for installation instructions, API documentation, and examples.

## Installation

To start using the OpenDXL Python client:

* Download the [Latest Release](https://github.com/opendxl/opendxl-client-python/releases/latest)
* Extract the release .zip file
* View the `README.html` file located at the root of the extracted files.
  * The `README` links to the SDK documentation which includes installation instructions, API details, and samples.
  * The SDK documentation is also available on-line [here](https://opendxl.github.io/opendxl-client-python/pydoc).

## Branches (this fork)

| Branch | Target environment | TLS cipher default |
|---|---|---|
| `master` | Trellix ePO On-prem 5.10.0 SP1 Update 7 or later with DXL Broker 6.1.1 or later (FIPS 140-3, OpenSSL 3.x, TLS 1.3 on the ePO side) | forward-secrecy suites only (`ECDHE+AESGCM:ECDHE+AES:DHE+AES:!aNULL:!eNULL`) |
| `epo-legacy` | Older ePO 5.10 updates and DXL Brokers up to 6.1.0 (MQTT listener offers only `AES128-SHA256`), also the open source `opendxl-broker` | forward-secrecy suites first, `AES128-SHA256` as fallback |

Both branches share all bug fixes; they differ only in the default of
`DxlClientConfig.tls_ciphers`. The default can be overridden on either branch
through the `TlsCiphers` setting in the `[General]` section of
`dxlclient.config` (see `dxlclient.client_config`). Trellix DXL 6.1.1 added
"strong ciphers along with weak ciphers" for MQTT (KB14602); brokers before that
release require the `epo-legacy` default or an explicit `TlsCiphers` value.

## Bugs and Feedback

For bugs, questions and discussions please use the [Github Issues](https://github.com/opendxl/opendxl-client-python/issues).

## LICENSE

Copyright 2024 Musarubra US LLC.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
