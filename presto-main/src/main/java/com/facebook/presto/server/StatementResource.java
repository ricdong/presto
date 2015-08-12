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

import com.facebook.presto.Session;
import com.facebook.presto.client.ClientTypeSignature;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StageStats;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.execution.BufferInfo;
import com.facebook.presto.execution.QueryId;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.SharedBufferInfo;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.StageState;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_SESSION;
import static com.facebook.presto.server.ResourceUtil.assertRequest;
import static com.facebook.presto.server.ResourceUtil.createSessionForRequest;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.toErrorType;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Path("/v1/statement")
public class StatementResource
{
    private static final Logger log = Logger.get(StatementResource.class);

    private static final Duration MAX_WAIT_TIME = new Duration(1, SECONDS);
    private static final Ordering<Comparable<Duration>> WAIT_ORDERING = Ordering.natural().nullsLast();
    private static final long DESIRED_RESULT_BYTES = new DataSize(1, MEGABYTE).toBytes();

    private final QueryManager queryManager;
    private final SessionPropertyManager sessionPropertyManager;
    private final Supplier<ExchangeClient> exchangeClientSupplier;

    private final ConcurrentMap<QueryId, Query> queries = new ConcurrentHashMap<>();
    private final ScheduledExecutorService queryPurger = newSingleThreadScheduledExecutor(threadsNamed("query-purger"));

    @Inject
    public StatementResource(QueryManager queryManager,
            SessionPropertyManager sessionPropertyManager,
            Supplier<ExchangeClient> exchangeClientSupplier)
    {
        this.queryManager = checkNotNull(queryManager, "queryManager is null");
        this.sessionPropertyManager = checkNotNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.exchangeClientSupplier = checkNotNull(exchangeClientSupplier, "exchangeClientSupplier is null");

        queryPurger.scheduleWithFixedDelay(new PurgeQueriesRunnable(queries, queryManager), 200, 200, MILLISECONDS);
    }

    @PreDestroy
    public void stop()
    {
        queryPurger.shutdownNow();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response createQuery(
            String statement,
            @Context HttpServletRequest servletRequest,
            @Context UriInfo uriInfo)
            throws InterruptedException
    {
        assertRequest(!isNullOrEmpty(statement), "SQL statement is empty");

        Session session = createSessionForRequest(servletRequest, sessionPropertyManager);

        ExchangeClient exchangeClient = exchangeClientSupplier.get();
        Query query = new Query(session, statement, queryManager, exchangeClient);
        queries.put(query.getQueryId(), query);

        return getQueryResults(query, Optional.empty(), uriInfo, new Duration(1, MILLISECONDS));
    }

    @GET
    @Path("{queryId}/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getQueryResults(
            @PathParam("queryId") QueryId queryId,
            @PathParam("token") long token,
            @QueryParam("maxWait") Duration maxWait,
            @Context UriInfo uriInfo)
            throws InterruptedException
    {
        Query query = queries.get(queryId);
        if (query == null) {
            return Response.status(Status.NOT_FOUND).build();
        }

        Duration wait = WAIT_ORDERING.min(MAX_WAIT_TIME, maxWait);
        return getQueryResults(query, Optional.of(token), uriInfo, wait);
    }

    private static Response getQueryResults(Query query, Optional<Long> token, UriInfo uriInfo, Duration wait)
            throws InterruptedException
    {
        QueryResults queryResults;
        if (token.isPresent()) {
            queryResults = query.getResults(token.get(), uriInfo, wait);
        }
        else {
            queryResults = query.getNextResults(uriInfo, wait);
        }

        ResponseBuilder response = Response.ok(queryResults);

        // add set session properties
        query.getSetSessionProperties().entrySet().stream()
                .forEach(entry -> response.header(PRESTO_SET_SESSION, entry.getKey() + '=' + entry.getValue()));

        // add clear session properties
        query.getResetSessionProperties().stream()
                .forEach(name -> response.header(PRESTO_CLEAR_SESSION, name));

        return response.build();
    }

    @DELETE
    @Path("{queryId}/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response cancelQuery(@PathParam("queryId") QueryId queryId,
            @PathParam("token") long token)
    {
        Query query = queries.get(queryId);
        if (query == null) {
            return Response.status(Status.NOT_FOUND).build();
        }
        query.cancel();
        return Response.noContent().build();
    }

    @ThreadSafe
    public static class Query
    {
        private final QueryManager queryManager;
        private final QueryId queryId;
        private final ExchangeClient exchangeClient;

        private final AtomicLong resultId = new AtomicLong();
        private final Session session;

        @GuardedBy("this")
        private QueryResults lastResult;

        @GuardedBy("this")
        private String lastResultPath;

        @GuardedBy("this")
        private List<Column> columns;

        @GuardedBy("this")
        private Map<String, String> setSessionProperties;

        @GuardedBy("this")
        private Set<String> resetSessionProperties;

        @GuardedBy("this")
        private Long updateCount;

        public Query(Session session,
                String query,
                QueryManager queryManager,
                ExchangeClient exchangeClient)
        {
            checkNotNull(session, "session is null");
            checkNotNull(query, "query is null");
            checkNotNull(queryManager, "queryManager is null");
            checkNotNull(exchangeClient, "exchangeClient is null");

            this.session = session;
            this.queryManager = queryManager;

            QueryInfo queryInfo = queryManager.createQuery(session, query);
            queryId = queryInfo.getQueryId();
            this.exchangeClient = exchangeClient;
        }

        public void cancel()
        {
            queryManager.cancelQuery(queryId);
            dispose();
        }

        public void dispose()
        {
            exchangeClient.close();
        }

        public QueryId getQueryId()
        {
            return queryId;
        }

        public synchronized Map<String, String> getSetSessionProperties()
        {
            return setSessionProperties;
        }

        public synchronized Set<String> getResetSessionProperties()
        {
            return resetSessionProperties;
        }

        public synchronized QueryResults getResults(long token, UriInfo uriInfo, Duration maxWaitTime)
                throws InterruptedException
        {
            // is the a repeated request for the last results?
            String requestedPath = uriInfo.getAbsolutePath().getPath();
            if (lastResultPath != null && requestedPath.equals(lastResultPath)) {
                // tell query manager we are still interested in the query
                queryManager.getQueryInfo(queryId);
                queryManager.recordHeartbeat(queryId);
                return lastResult;
            }

            if (token < resultId.get()) {
                throw new WebApplicationException(Status.GONE);
            }

            // if this is not a request for the next results, return not found
            if (lastResult.getNextUri() == null || !requestedPath.equals(lastResult.getNextUri().getPath())) {
                // unknown token
                throw new WebApplicationException(Status.NOT_FOUND);
            }

            return getNextResults(uriInfo, maxWaitTime);
        }

        public synchronized QueryResults getNextResults(UriInfo uriInfo, Duration maxWaitTime)
                throws InterruptedException
        {
            Iterable<List<Object>> data = getData(maxWaitTime);

            // get the query info before returning
            // force update if query manager is closed
            QueryInfo queryInfo = queryManager.getQueryInfo(queryId);
            queryManager.recordHeartbeat(queryId);

            // if we have received all of the output data and the query is not marked as done, wait for the query to finish
            if (exchangeClient.isClosed() && !queryInfo.getState().isDone()) {
                queryManager.waitForStateChange(queryId, queryInfo.getState(), maxWaitTime);
                queryInfo = queryManager.getQueryInfo(queryId);
            }

            // TODO: figure out a better way to do this
            // grab the update count for non-queries
            if ((data != null) && (queryInfo.getUpdateType() != null) && (updateCount == null) &&
                    (columns.size() == 1) && (columns.get(0).getType().equals(StandardTypes.BIGINT))) {
                Iterator<List<Object>> iterator = data.iterator();
                if (iterator.hasNext()) {
                    updateCount = ((Number) iterator.next().get(0)).longValue();
                }
            }

            // close exchange client if the query has failed
            if (queryInfo.getState().isDone()) {
                if (queryInfo.getState() != QueryState.FINISHED) {
                    exchangeClient.close();
                }
                else if (queryInfo.getOutputStage() == null) {
                    // For simple executions (e.g. drop table), there will never be an output stage,
                    // so close the exchange as soon as the query is done.
                    exchangeClient.close();

                    // Return a single value for clients that require a result.
                    columns = ImmutableList.of(new Column("result", "boolean", new ClientTypeSignature(StandardTypes.BOOLEAN, ImmutableList.<ClientTypeSignature>of(), ImmutableList.of())));
                    data = ImmutableSet.<List<Object>>of(ImmutableList.<Object>of(true));
                }
            }

            // only return a next if the query is not done or there is more data to send (due to buffering)
            URI nextResultsUri = null;
            if ((!queryInfo.getState().isDone()) || (!exchangeClient.isClosed())) {
                nextResultsUri = createNextResultsUri(uriInfo);
            }

            // update setSessionProperties
            setSessionProperties = queryInfo.getSetSessionProperties();
            resetSessionProperties = queryInfo.getResetSessionProperties();

            // first time through, self is null
            QueryResults queryResults = new QueryResults(
                    queryId.toString(),
                    uriInfo.getRequestUriBuilder().replaceQuery("").replacePath(queryInfo.getSelf().getPath()).build(),
                    findCancelableLeafStage(queryInfo),
                    nextResultsUri,
                    columns,
                    data,
                    toStatementStats(queryInfo),
                    toQueryError(queryInfo),
                    queryInfo.getUpdateType(),
                    updateCount);

            // cache the last results
            if (lastResult != null && lastResult.getNextUri() != null) {
                lastResultPath = lastResult.getNextUri().getPath();
            }
            else {
                lastResultPath = null;
            }
            lastResult = queryResults;
            return queryResults;
        }

        private synchronized Iterable<List<Object>> getData(Duration maxWait)
                throws InterruptedException
        {
            // wait for query to start
            QueryInfo queryInfo = queryManager.getQueryInfo(queryId);
            while (maxWait.toMillis() > 1 && !isQueryStarted(queryInfo)) {
                queryManager.recordHeartbeat(queryId);
                maxWait = queryManager.waitForStateChange(queryId, queryInfo.getState(), maxWait);
                queryInfo = queryManager.getQueryInfo(queryId);
            }

            // if query did not finish starting or does not have output, just return
            if (!isQueryStarted(queryInfo) || queryInfo.getOutputStage() == null) {
                return null;
            }

            if (columns == null) {
                columns = createColumnsList(queryInfo);
            }

            List<Type> types = queryInfo.getOutputStage().getTypes();

            updateExchangeClient(queryInfo.getOutputStage());

            ImmutableList.Builder<RowIterable> pages = ImmutableList.builder();
            // wait up to max wait for data to arrive; then try to return at least DESIRED_RESULT_BYTES
            long bytes = 0;
            while (bytes < DESIRED_RESULT_BYTES) {
                Page page = exchangeClient.getNextPage(maxWait);
                if (page == null) {
                    break;
                }
                bytes += page.getSizeInBytes();
                pages.add(new RowIterable(session.toConnectorSession(), types, page));

                // only wait on first call
                maxWait = new Duration(0, MILLISECONDS);
            }

            if (bytes == 0) {
                return null;
            }

            return Iterables.concat(pages.build());
        }

        private static boolean isQueryStarted(QueryInfo queryInfo)
        {
            QueryState state = queryInfo.getState();
            return state != QueryState.QUEUED && queryInfo.getState() != QueryState.PLANNING && queryInfo.getState() != QueryState.STARTING;
        }

        private synchronized void updateExchangeClient(StageInfo outputStage)
        {
            // add any additional output locations
            if (!outputStage.getState().isDone()) {
                for (TaskInfo taskInfo : outputStage.getTasks()) {
                    SharedBufferInfo outputBuffers = taskInfo.getOutputBuffers();
                    List<BufferInfo> buffers = outputBuffers.getBuffers();
                    if (buffers.isEmpty() || outputBuffers.getState().canAddBuffers()) {
                        // output buffer has not been created yet
                        continue;
                    }
                    Preconditions.checkState(buffers.size() == 1,
                            "Expected a single output buffer for task %s, but found %s",
                            taskInfo.getTaskId(),
                            buffers);

                    TaskId bufferId = Iterables.getOnlyElement(buffers).getBufferId();
                    URI uri = uriBuilderFrom(taskInfo.getSelf()).appendPath("results").appendPath(bufferId.toString()).build();
                    exchangeClient.addLocation(uri);
                }
            }

            if (allOutputBuffersCreated(outputStage)) {
                exchangeClient.noMoreLocations();
            }
        }

        private static boolean allOutputBuffersCreated(StageInfo outputStage)
        {
            StageState stageState = outputStage.getState();

            // if the stage is already done, then there will be no more buffers
            if (stageState.isDone()) {
                return true;
            }

            // has the stage finished scheduling?
            if (stageState == StageState.PLANNED || stageState == StageState.SCHEDULING) {
                return false;
            }

            // have all tasks finished adding buffers
            return outputStage.getTasks().stream()
                    .allMatch(taskInfo -> !taskInfo.getOutputBuffers().getState().canAddBuffers());
        }

        private synchronized URI createNextResultsUri(UriInfo uriInfo)
        {
            return uriInfo.getBaseUriBuilder().replacePath("/v1/statement").path(queryId.toString()).path(String.valueOf(resultId.incrementAndGet())).replaceQuery("").build();
        }

        private static List<Column> createColumnsList(QueryInfo queryInfo)
        {
            checkNotNull(queryInfo, "queryInfo is null");
            StageInfo outputStage = queryInfo.getOutputStage();
            checkNotNull(outputStage, "outputStage is null");

            List<String> names = queryInfo.getFieldNames();
            List<Type> types = outputStage.getTypes();

            checkArgument(names.size() == types.size(), "names and types size mismatch");

            ImmutableList.Builder<Column> list = ImmutableList.builder();
            for (int i = 0; i < names.size(); i++) {
                String name = names.get(i);
                TypeSignature typeSignature = types.get(i).getTypeSignature();
                String type = typeSignature.toString();
                list.add(new Column(name, type, new ClientTypeSignature(typeSignature)));
            }
            return list.build();
        }

        private static StatementStats toStatementStats(QueryInfo queryInfo)
        {
            QueryStats queryStats = queryInfo.getQueryStats();

            return StatementStats.builder()
                    .setState(queryInfo.getState().toString())
                    .setScheduled(queryInfo.isScheduled())
                    .setNodes(globalUniqueNodes(queryInfo.getOutputStage()).size())
                    .setTotalSplits(queryStats.getTotalDrivers())
                    .setQueuedSplits(queryStats.getQueuedDrivers())
                    .setRunningSplits(queryStats.getRunningDrivers())
                    .setCompletedSplits(queryStats.getCompletedDrivers())
                    .setUserTimeMillis(queryStats.getTotalUserTime().toMillis())
                    .setCpuTimeMillis(queryStats.getTotalCpuTime().toMillis())
                    .setWallTimeMillis(queryStats.getTotalScheduledTime().toMillis())
                    .setProcessedRows(queryStats.getRawInputPositions())
                    .setProcessedBytes(queryStats.getRawInputDataSize().toBytes())
                    .setRootStage(toStageStats(queryInfo.getOutputStage()))
                    .build();
        }

        private static StageStats toStageStats(StageInfo stageInfo)
        {
            if (stageInfo == null) {
                return null;
            }

            com.facebook.presto.execution.StageStats stageStats = stageInfo.getStageStats();

            ImmutableList.Builder<StageStats> subStages = ImmutableList.builder();
            for (StageInfo subStage : stageInfo.getSubStages()) {
                subStages.add(toStageStats(subStage));
            }

            Set<String> uniqueNodes = new HashSet<>();
            for (TaskInfo task : stageInfo.getTasks()) {
                // todo add nodeId to TaskInfo
                URI uri = task.getSelf();
                uniqueNodes.add(uri.getHost() + ":" + uri.getPort());
            }

            return StageStats.builder()
                    .setStageId(String.valueOf(stageInfo.getStageId().getId()))
                    .setState(stageInfo.getState().toString())
                    .setDone(stageInfo.getState().isDone())
                    .setNodes(uniqueNodes.size())
                    .setTotalSplits(stageStats.getTotalDrivers())
                    .setQueuedSplits(stageStats.getQueuedDrivers())
                    .setRunningSplits(stageStats.getRunningDrivers())
                    .setCompletedSplits(stageStats.getCompletedDrivers())
                    .setUserTimeMillis(stageStats.getTotalUserTime().toMillis())
                    .setCpuTimeMillis(stageStats.getTotalCpuTime().toMillis())
                    .setWallTimeMillis(stageStats.getTotalScheduledTime().toMillis())
                    .setProcessedRows(stageStats.getRawInputPositions())
                    .setProcessedBytes(stageStats.getRawInputDataSize().toBytes())
                    .setSubStages(subStages.build())
                    .build();
        }

        private static Set<String> globalUniqueNodes(StageInfo stageInfo)
        {
            if (stageInfo == null) {
                return ImmutableSet.of();
            }
            ImmutableSet.Builder<String> nodes = ImmutableSet.builder();
            for (TaskInfo task : stageInfo.getTasks()) {
                // todo add nodeId to TaskInfo
                URI uri = task.getSelf();
                nodes.add(uri.getHost() + ":" + uri.getPort());
            }

            for (StageInfo subStage : stageInfo.getSubStages()) {
                nodes.addAll(globalUniqueNodes(subStage));
            }
            return nodes.build();
        }

        private static URI findCancelableLeafStage(QueryInfo queryInfo)
        {
            if (queryInfo.getOutputStage() == null) {
                // query is not running yet, cannot cancel leaf stage
                return null;
            }

            // query is running, find the leaf-most running stage
            return findCancelableLeafStage(queryInfo.getOutputStage());
        }

        private static URI findCancelableLeafStage(StageInfo stage)
        {
            // if this stage is already done, we can't cancel it
            if (stage.getState().isDone()) {
                return null;
            }

            // attempt to find a cancelable sub stage
            // check in reverse order since build side of a join will be later in the list
            for (StageInfo subStage : Lists.reverse(stage.getSubStages())) {
                URI leafStage = findCancelableLeafStage(subStage);
                if (leafStage != null) {
                    return leafStage;
                }
            }

            // no matching sub stage, so return this stage
            return stage.getSelf();
        }

        private static QueryError toQueryError(QueryInfo queryInfo)
        {
            FailureInfo failure = queryInfo.getFailureInfo();
            if (failure == null) {
                QueryState state = queryInfo.getState();
                if ((!state.isDone()) || (state == QueryState.FINISHED)) {
                    return null;
                }
                log.warn("Query %s in state %s has no failure info", queryInfo.getQueryId(), state);
                failure = toFailure(new RuntimeException(format("Query is %s (reason unknown)", state))).toFailureInfo();
            }

            ErrorCode errorCode;
            if (queryInfo.getErrorCode() != null) {
                errorCode = queryInfo.getErrorCode();
            }
            else {
                errorCode = INTERNAL_ERROR.toErrorCode();
                log.warn("Failed query %s has no error code", queryInfo.getQueryId());
            }
            return new QueryError(
                    failure.getMessage(),
                    null,
                    errorCode.getCode(),
                    errorCode.getName(),
                    toErrorType(errorCode.getCode()).toString(),
                    failure.getErrorLocation(),
                    failure);
        }

        private static class RowIterable
                implements Iterable<List<Object>>
        {
            private final ConnectorSession session;
            private final List<Type> types;
            private final Page page;

            private RowIterable(ConnectorSession session, List<Type> types, Page page)
            {
                this.session = session;
                this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
                this.page = checkNotNull(page, "page is null");
            }

            @Override
            public Iterator<List<Object>> iterator()
            {
                return new RowIterator(session, types, page);
            }
        }

        private static class RowIterator
                extends AbstractIterator<List<Object>>
        {
            private final ConnectorSession session;
            private final List<Type> types;
            private final Page page;
            private int position = -1;

            private RowIterator(ConnectorSession session, List<Type> types, Page page)
            {
                this.session = session;
                this.types = types;
                this.page = page;
            }

            @Override
            protected List<Object> computeNext()
            {
                position++;
                if (position >= page.getPositionCount()) {
                    return endOfData();
                }

                List<Object> values = new ArrayList<>(page.getChannelCount());
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    Type type = types.get(channel);
                    Block block = page.getBlock(channel);
                    values.add(type.getObjectValue(session, block, position));
                }
                return Collections.unmodifiableList(values);
            }
        }
    }

    private static class PurgeQueriesRunnable
            implements Runnable
    {
        private final ConcurrentMap<QueryId, Query> queries;
        private final QueryManager queryManager;

        public PurgeQueriesRunnable(ConcurrentMap<QueryId, Query> queries, QueryManager queryManager)
        {
            this.queries = queries;
            this.queryManager = queryManager;
        }

        @Override
        public void run()
        {
            try {
                // Queries are added to the query manager before being recorded in queryIds set.
                // Therefore, we take a snapshot if queryIds before getting the live queries
                // from the query manager.  Then we remove only the queries in the snapshot and
                // not live queries set.  If we did this in the other order, a query could be
                // registered between fetching the live queries and inspecting the queryIds set.
                for (QueryId queryId : ImmutableSet.copyOf(queries.keySet())) {
                    Query query = queries.get(queryId);
                    Optional<QueryState> state = queryManager.getQueryState(queryId);

                    // free up resources if the query completed
                    if (!state.isPresent() || state.get() == QueryState.FAILED) {
                        query.dispose();
                    }

                    // forget about this query if the query manager is no longer tracking it
                    if (!state.isPresent()) {
                        queries.remove(queryId);
                    }
                }
            }
            catch (Throwable e) {
                log.warn(e, "Error removing old queries");
            }
        }
    }
}
