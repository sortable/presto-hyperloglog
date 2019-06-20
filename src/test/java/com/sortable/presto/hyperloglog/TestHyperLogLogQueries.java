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

package com.sortable.presto.hyperloglog;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.ParametricType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestHyperLogLogQueries
        extends AbstractTestQueryFramework
{
    public TestHyperLogLogQueries()
    {
        super(TestHyperLogLogQueries::createLocalQueryRunner);
    }

    @Test
    public void testHyperLogLogMerge()
            throws Exception
    {
        assertQuery("with x as (select k, approx_set(v) as hll from (values ('US', 'foo'), ('US', 'bar'), ('US', 'ok'), ('IT', 'foo')) as t(k, v) group by k) " +
                        "select cast(cast(merge_p4(hll) as hyperloglog) as varbinary) = cast(cast(cast(merge(hll) as p4hyperloglog) as hyperloglog) as varbinary) from x",
                "select true");
    }

    @Test
    public void testHyperLogLogGroupMerge()
            throws Exception
    {
        assertQuery("with x as (select k, approx_set(v) as hll from (values ('US', 'foo'), ('US', 'bar'), ('US', 'ok'), ('IT', 'foo')) as t(k, v) group by k) " +
                        "select k, cast(cast(merge_p4(hll) as hyperloglog) as varbinary) = cast(cast(cast(merge(hll) as p4hyperloglog) as hyperloglog) as varbinary) from x group by 1",
                "values ('US', true), ('IT', true)");
    }

    private static LocalQueryRunner createLocalQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.<String, String>of());

        HyperLogLogPlugin plugin = new HyperLogLogPlugin();
        for (Type type : plugin.getTypes()) {
            localQueryRunner.getTypeManager().addType(type);
        }
        for (ParametricType parametricType : plugin.getParametricTypes()) {
            localQueryRunner.getTypeManager().addParametricType(parametricType);
        }

        localQueryRunner.getMetadata().addFunctions(extractFunctions(plugin.getFunctions()));

        return localQueryRunner;
    }
}
