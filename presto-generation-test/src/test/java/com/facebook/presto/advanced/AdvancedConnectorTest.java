/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.advanced;

import com.facebook.presto.framework.ConnectorTestFramework;
import com.facebook.presto.test.TestableConnector;

import static com.facebook.presto.test.ConnectorFeature.READ_DATA;
import static com.facebook.presto.test.ConnectorFeature.WRITE_DATA;

@TestableConnector(connectorName = "AdvancedConnector", supportedFeatures = {READ_DATA, WRITE_DATA})
public class AdvancedConnectorTest
{
    private AdvancedConnectorTest()
    {
    }

    public static ConnectorTestFramework.ConnectorSupplier getSupplier()
    {
        return AdvancedConnector::new;
    }
}
