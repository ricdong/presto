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
package com.facebook.presto.server;

import com.facebook.presto.block.PagesSerde;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.RuntimeIOException;

import javax.inject.Inject;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.List;

import static com.facebook.presto.PrestoMediaTypes.PRESTO_PAGES;

@Provider
@Produces(PRESTO_PAGES)
public class PagesResponseWriter
        implements MessageBodyWriter<List<Page>>
{
    private static final MediaType PRESTO_PAGES_TYPE = MediaType.valueOf(PRESTO_PAGES);
    private static final Type LIST_GENERIC_TOKEN;

    static {
        try {
            LIST_GENERIC_TOKEN = List.class.getMethod("get", int.class).getGenericReturnType();
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    private final BlockEncodingSerde blockEncodingSerde;

    @Inject
    public PagesResponseWriter(BlockEncodingSerde blockEncodingSerde)
    {
        this.blockEncodingSerde = blockEncodingSerde;
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return List.class.isAssignableFrom(type) &&
                TypeToken.of(genericType).resolveType(LIST_GENERIC_TOKEN).getRawType().equals(Page.class) &&
                mediaType.isCompatible(PRESTO_PAGES_TYPE);
    }

    @Override
    public long getSize(List<Page> pages, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return -1;
    }

    @Override
    public void writeTo(List<Page> pages,
            Class<?> type,
            Type genericType,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, Object> httpHeaders,
            OutputStream output)
            throws IOException, WebApplicationException
    {
        try {
            PagesSerde.writePages(blockEncodingSerde, new OutputStreamSliceOutput(output), pages);
        }
        catch (RuntimeIOException e) {
            // EOF exception occurs when the client disconnects while writing data
            // This is not a "server" problem so we don't want to log this
            if (!(e.getCause() instanceof EOFException)) {
                throw e;
            }
        }
    }
}
