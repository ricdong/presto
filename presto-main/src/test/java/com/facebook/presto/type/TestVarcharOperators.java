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
package com.facebook.presto.type;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestVarcharOperators
        extends AbstractTestFunctions
{
    @Test
    public void testLiteral()
            throws Exception
    {
        assertFunction("'foo'", VARCHAR, "foo");
        assertFunction("'bar'", VARCHAR, "bar");
        assertFunction("''", VARCHAR, "");
    }

    @Test
    public void testAdd()
            throws Exception
    {
        assertFunction("'foo' || 'foo'", VARCHAR, "foo" + "foo");
        assertFunction("'foo' || 'bar'", VARCHAR, "foo" + "bar");
        assertFunction("'bar' || 'foo'", VARCHAR, "bar" + "foo");
        assertFunction("'bar' || 'bar'", VARCHAR, "bar" + "bar");
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("'foo' = 'foo'", BOOLEAN, true);
        assertFunction("'foo' = 'bar'", BOOLEAN, false);
        assertFunction("'bar' = 'foo'", BOOLEAN, false);
        assertFunction("'bar' = 'bar'", BOOLEAN, true);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("'foo' <> 'foo'", BOOLEAN, false);
        assertFunction("'foo' <> 'bar'", BOOLEAN, true);
        assertFunction("'bar' <> 'foo'", BOOLEAN, true);
        assertFunction("'bar' <> 'bar'", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("'foo' < 'foo'", BOOLEAN, false);
        assertFunction("'foo' < 'bar'", BOOLEAN, false);
        assertFunction("'bar' < 'foo'", BOOLEAN, true);
        assertFunction("'bar' < 'bar'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("'foo' <= 'foo'", BOOLEAN, true);
        assertFunction("'foo' <= 'bar'", BOOLEAN, false);
        assertFunction("'bar' <= 'foo'", BOOLEAN, true);
        assertFunction("'bar' <= 'bar'", BOOLEAN, true);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("'foo' > 'foo'", BOOLEAN, false);
        assertFunction("'foo' > 'bar'", BOOLEAN, true);
        assertFunction("'bar' > 'foo'", BOOLEAN, false);
        assertFunction("'bar' > 'bar'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("'foo' >= 'foo'", BOOLEAN, true);
        assertFunction("'foo' >= 'bar'", BOOLEAN, true);
        assertFunction("'bar' >= 'foo'", BOOLEAN, false);
        assertFunction("'bar' >= 'bar'", BOOLEAN, true);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("'foo' BETWEEN 'foo' AND 'foo'", BOOLEAN, true);
        assertFunction("'foo' BETWEEN 'foo' AND 'bar'", BOOLEAN, false);

        assertFunction("'foo' BETWEEN 'bar' AND 'foo'", BOOLEAN, true);
        assertFunction("'foo' BETWEEN 'bar' AND 'bar'", BOOLEAN, false);

        assertFunction("'bar' BETWEEN 'foo' AND 'foo'", BOOLEAN, false);
        assertFunction("'bar' BETWEEN 'foo' AND 'bar'", BOOLEAN, false);

        assertFunction("'bar' BETWEEN 'bar' AND 'foo'", BOOLEAN, true);
        assertFunction("'bar' BETWEEN 'bar' AND 'bar'", BOOLEAN, true);
    }
}
