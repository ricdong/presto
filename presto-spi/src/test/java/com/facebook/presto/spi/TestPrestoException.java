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
package com.facebook.presto.spi;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestPrestoException
{
    @Test
    public void testMessage()
    {
        PrestoException exception = new PrestoException(new TestErrorCode(), "test");
        Assert.assertEquals(exception.getMessage(), "test");

        exception = new PrestoException(new TestErrorCode(), new RuntimeException("test2"));
        Assert.assertEquals(exception.getMessage(), "test2");

        exception = new PrestoException(new TestErrorCode(), new RuntimeException());
        Assert.assertEquals(exception.getMessage(), "test");
    }

    private static class TestErrorCode
            implements ErrorCodeSupplier
    {
        @Override
        public ErrorCode toErrorCode()
        {
            return new ErrorCode(0, "test");
        }
    }
}
