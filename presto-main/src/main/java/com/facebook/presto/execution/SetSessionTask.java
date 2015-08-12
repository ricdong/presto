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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.SetSession;

import static com.facebook.presto.metadata.SessionPropertyManager.evaluatePropertyValue;
import static com.facebook.presto.metadata.SessionPropertyManager.serializeSessionProperty;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_SESSION_PROPERTY;

public class SetSessionTask
        implements DataDefinitionTask<SetSession>
{
    @Override
    public String getName()
    {
        return "SET SESSION";
    }

    @Override
    public void execute(SetSession statement, Session session, Metadata metadata, QueryStateMachine stateMachine)
    {
        if (statement.getName().getParts().size() > 2) {
            throw new SemanticException(INVALID_SESSION_PROPERTY, statement, "Invalid session property '%s'", statement.getName());
        }
        String name = statement.getName().toString();

        PropertyMetadata<?> propertyMetadata = metadata.getSessionPropertyManager().getSessionPropertyMetadata(name);
        Type type = propertyMetadata.getSqlType();

        Object objectValue = evaluatePropertyValue(statement.getValue(), type, session, metadata);
        String value = serializeSessionProperty(type, objectValue);

        // verify the SQL value can be decoded by the property
        metadata.getSessionPropertyManager().decodeProperty(name, value, propertyMetadata.getJavaType());

        stateMachine.addSetSessionProperties(name, value);
    }
}
