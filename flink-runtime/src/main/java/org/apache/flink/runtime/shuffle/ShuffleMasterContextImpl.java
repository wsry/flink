/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.shuffle;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The default implementation of {@link ShuffleMasterContext}. */
public class ShuffleMasterContextImpl implements ShuffleMasterContext {

    private final Configuration configuration;

    private final GatewayRetriever<ResourceManagerGateway> rmGatewayRetriever;

    private final FatalErrorHandler fatalErrorHandler;

    public ShuffleMasterContextImpl(
            Configuration configuration,
            GatewayRetriever<ResourceManagerGateway> rmGatewayRetriever,
            FatalErrorHandler fatalErrorHandler) {
        this.configuration = checkNotNull(configuration);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.rmGatewayRetriever = checkNotNull(rmGatewayRetriever);
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public void onFatalError(Throwable throwable) {
        fatalErrorHandler.onFatalError(throwable);
    }

    @Override
    public CompletableFuture<?> stopTrackingDataSet(IntermediateDataSetID dataSetID) {
        CompletableFuture<?> future = new CompletableFuture<>();
        try {
            getResourceManagerGateway()
                    .releaseClusterPartitions(dataSetID)
                    .whenComplete(
                            (ignored, throwable) -> {
                                if (throwable != null) {
                                    future.completeExceptionally(throwable);
                                    return;
                                }
                                future.complete(null);
                            });
        } catch (Throwable throwable) {
            future.completeExceptionally(throwable);
        }
        return future;
    }

    @Override
    public CompletableFuture<List<IntermediateDataSetID>> listDataSets() {
        CompletableFuture<List<IntermediateDataSetID>> future = new CompletableFuture<>();
        try {
            getResourceManagerGateway()
                    .listDataSets()
                    .whenComplete(
                            (datasetInfo, throwable) -> {
                                if (throwable != null) {
                                    future.completeExceptionally(throwable);
                                    return;
                                }
                                future.complete(new ArrayList<>(datasetInfo.keySet()));
                            });
        } catch (Throwable throwable) {
            future.completeExceptionally(throwable);
        }
        return future;
    }

    private ResourceManagerGateway getResourceManagerGateway() {
        Optional<ResourceManagerGateway> optionalGateway = rmGatewayRetriever.getNow();
        if (!optionalGateway.isPresent()) {
            throw new RuntimeException("Resource manager is not available currently.");
        }
        return optionalGateway.get();
    }
}
