/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.inject.ImplementedBy;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ScrollQueryFetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchRequest;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.search.query.ScrollQuerySearchResult;

@ImplementedBy(InternalSearchService.class)
public interface SearchService {
    DfsSearchResult executeDfsPhase(ShardSearchRequest request) throws ElasticsearchException;

    QuerySearchResult executeScan(ShardSearchRequest request) throws ElasticsearchException;

    ScrollQueryFetchSearchResult executeScan(InternalScrollSearchRequest request) throws ElasticsearchException;

    QuerySearchResultProvider executeQueryPhase(ShardSearchRequest request) throws ElasticsearchException;

    ScrollQuerySearchResult executeQueryPhase(InternalScrollSearchRequest request) throws ElasticsearchException;

    QuerySearchResult executeQueryPhase(QuerySearchRequest request) throws ElasticsearchException;

    QueryFetchSearchResult executeFetchPhase(ShardSearchRequest request) throws ElasticsearchException;

    QueryFetchSearchResult executeFetchPhase(QuerySearchRequest request) throws ElasticsearchException;

    ScrollQueryFetchSearchResult executeFetchPhase(InternalScrollSearchRequest request) throws ElasticsearchException;

    FetchSearchResult executeFetchPhase(ShardFetchRequest request) throws ElasticsearchException;

    boolean freeContext(long id);

    void freeAllScrollContexts();

    int getActiveContexts();
}
