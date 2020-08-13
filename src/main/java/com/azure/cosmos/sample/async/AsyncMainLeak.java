// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.sample.async;

import com.azure.cosmos.*;
import com.azure.cosmos.models.*;
import com.azure.cosmos.sample.common.AccountSettings;
import com.azure.cosmos.sample.common.Families;
import com.azure.cosmos.sample.common.Family;
import com.azure.cosmos.sample.common.VersionedJsonNode;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.util.ResourceLeakDetector;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;

public class AsyncMainLeak {

    private CosmosAsyncClient client1;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName1 = "FamilyContainer1";
    private final String containerName2 = "FamilyContainer2";
    private final String containerName3 = "FamilyContainer3";

    private CosmosAsyncDatabase database1;

    private CosmosAsyncContainer container1;
    private CosmosAsyncContainer container2;
    private CosmosAsyncContainer container3;

    private ObjectMapper mapper = new ObjectMapper();

    private int queryCount = 5;

    public void close() {
        client1.close();
    }

    /**
     * Run a Hello CosmosDB console application.
     *
     * @param args command line args.
     */
    //  <Main>
    public static void main(String[] args) {
        AsyncMainLeak p = new AsyncMainLeak();

        try {
            System.out.println("Starting ASYNC main");
            p.getStartedDemo();
            System.out.println("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(String.format("Cosmos getStarted failed with %s", e));
        } finally {
            System.out.println("Closing the client");
            p.close();
        }
        System.exit(0);
    }

    //  </Main>

    private CosmosAsyncClient createClient() {
        return new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .gatewayMode()
                .key(AccountSettings.MASTER_KEY)
                //  Setting the preferred location to Cosmos DB Account region
                //  West US is just an example. User should set preferred location to the Cosmos DB region closest to the application
//                .preferredRegions(Collections.singletonList("West US"))
//            .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .consistencyLevel(ConsistencyLevel.SESSION)
                //  Setting content response on write enabled, which enables the SDK to return response on write operations.
                .contentResponseOnWriteEnabled(true)
                .buildAsyncClient();
    }

    private void getStartedDemo() throws Exception {
        System.out.println("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        RecordingLeakDetectorFactory.register();
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        Family andersenFamilyItem = Families.getAndersenFamilyItem();
        Family wakefieldFamilyItem = Families.getWakefieldFamilyItem();
        Family johnsonFamilyItem = Families.getJohnsonFamilyItem();
        Family smithFamilyItem = Families.getSmithFamilyItem();

        //convert to JsonNode (we use generic JSON documents for our db)
        JsonNode andersenFamilyJsonNode = mapper.reader().readTree(mapper.writer().writeValueAsString(andersenFamilyItem));
        JsonNode wakefieldFamilyJsonNode = mapper.reader().readTree(mapper.writer().writeValueAsString(wakefieldFamilyItem));
        JsonNode johnsonFamilyJsonNode = mapper.reader().readTree(mapper.writer().writeValueAsString(johnsonFamilyItem));
        JsonNode smithFamilyJsonNode = mapper.reader().readTree(mapper.writer().writeValueAsString(smithFamilyItem));

        //init container1
        client1 = createClient();
        database1 = createDatabaseIfNotExists(client1);
        container1 = createContainerIfNotExists(database1, containerName1);


        //create 1 (whisks)
        VersionedJsonNode familyJsonNode = createItem(container1, andersenFamilyJsonNode);

        //query 6
        for (int i = 0; i < queryCount; i++) {
            queryItems(container1);
        }
        //delete 1 (whisks)
        deleteFamily(container1, familyJsonNode, false);

        //create 1 (whisks)
        VersionedJsonNode familyJsonNode2 = createItem(container1, wakefieldFamilyJsonNode);
        //delete 1 -> 204 (whisks)
        deleteFamily(container1, familyJsonNode2, false);
        //query 6
        for (int i = 0; i < queryCount; i++) {
            queryItems(container1);
        }
        //delete 1 -> 404 (whisks)
        try {
            deleteFamily(container1, familyJsonNode2, true);
        } catch (RuntimeException re) {
            //this is expected
        }

        //init container2
        container2 = createContainerIfNotExists(database1, containerName2);

        //create 1 (subjects)
        VersionedJsonNode familyJsonNode3 = createItem(container2, johnsonFamilyJsonNode);
        //get 1 (subjects)
        VersionedJsonNode familyJsonNode4 = readItem(container2, johnsonFamilyItem.getId(), johnsonFamilyItem.getLastName());
        //replace 1  (subjects)
        ((ObjectNode) familyJsonNode4.getJsonNode()).put("district", "updated district");
        VersionedJsonNode familyJsonNode5 = replaceItem(container2, familyJsonNode4);

        //init container3
        container3 = createContainerIfNotExists(database1, containerName3);

        //create 1 (activations)
        VersionedJsonNode familyJsonNode6 = createItem(container3, wakefieldFamilyJsonNode);
        //query (activations
        queryItems(container3);

        //delete 1 (whisks, 404)
        andersenFamilyItem.setId("bad1");
        VersionedJsonNode toDelete1 = new VersionedJsonNode(mapper.reader().readTree(mapper.writer().writeValueAsString(andersenFamilyItem)),
                familyJsonNode.getEtag());
        andersenFamilyItem.setId("bad2");
        VersionedJsonNode toDelete2 = new VersionedJsonNode(mapper.reader().readTree(mapper.writer().writeValueAsString(andersenFamilyItem)),
                familyJsonNode.getEtag());

        //delete 1 (whisks, 404)
        try {
            deleteFamily(container1, toDelete1, true);
        } catch (RuntimeException re) {
            //this is expected
        }
        //delete 1 (whisks, 404)
        try {
            deleteFamily(container1, toDelete2, true);
        } catch (RuntimeException re) {
            //this is expected
        }
        //delete 1 (subjects, 412)
        VersionedJsonNode badEtag = new VersionedJsonNode(familyJsonNode4.getJsonNode(), "bad-etag");
        try {
            deleteFamily(container2, badEtag, true);
        } catch (RuntimeException re) {
            //this is expected
        }
        //delete 1 (subjects, 204)
        deleteFamily(container2, familyJsonNode5, false);
        //delete 1 (activations, 204)
        deleteFamily(container3, familyJsonNode6, false);


        //System.gc();

        System.out.println("leak counter " + RecordingLeakDetectorFactory.counter.get());
    }

    private CosmosAsyncDatabase createDatabaseIfNotExists(CosmosAsyncClient client) throws Exception {
        System.out.println("Create database " + databaseName + " if not exists.");

        //  Create database if not exists
        //  <CreateDatabaseIfNotExists>
        Mono<CosmosDatabaseResponse> databaseResponseMono = client.createDatabaseIfNotExists(databaseName);
        return databaseResponseMono.flatMap(databaseResponse -> {
            CosmosAsyncDatabase database = client.getDatabase(databaseResponse.getProperties().getId());
            System.out.println("Checking database " + database.getId() + " completed!\n");
            return Mono.just(database);
        }).block();
        //  </CreateDatabaseIfNotExists>
    }

    private CosmosAsyncContainer createContainerIfNotExists(CosmosAsyncDatabase database, String containerName) throws Exception {
        System.out.println("Create container " + containerName + " if not exists.");

        //  Create container if not exists
        //  <CreateContainerIfNotExists>

        CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerName, "/lastName");
        Mono<CosmosContainerResponse> containerResponseMono = database.createContainerIfNotExists(containerProperties, ThroughputProperties.createManualThroughput(400));

        //  Create container with 400 RU/s
        return containerResponseMono.flatMap(containerResponse -> {
            CosmosAsyncContainer container = database.getContainer(containerResponse.getProperties().getId());
            System.out.println("Checking container " + container.getId() + " completed!\n");
            return Mono.just(container);
        }).block();

        //  </CreateContainerIfNotExists>
    }


    private void queryItems(CosmosAsyncContainer container) {
        //  <QueryItems>
        // Set some common query options

        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        //  Set query metrics enabled to get metrics around query executions
        queryOptions.setQueryMetricsEnabled(true);

        System.out.println("### QUERY");
        CosmosPagedFlux<JsonNode> pagedFluxResponse = container.queryItems(
                "SELECT * FROM Family WHERE Family.lastName IN ('Andersen', 'Wakefield', 'Johnson')", queryOptions, JsonNode.class);

        final CountDownLatch completionLatch = new CountDownLatch(1);

        pagedFluxResponse.byPage(10).subscribe(
                fluxResponse -> {
                    System.out.println("   QUERY Got a page of query result with " +
                            fluxResponse.getResults().size() + " items(s)"
                            + " and request charge of " + fluxResponse.getRequestCharge());
                },
                err -> {
                    if (err instanceof CosmosException) {
                        //Client-specific errors
                        CosmosException cerr = (CosmosException) err;
                        cerr.printStackTrace();
                        System.err.println(String.format("Read Item failed with %s\n", cerr));
                    } else {
                        //General errors
                        err.printStackTrace();
                    }

                    completionLatch.countDown();
                },
                () -> {
                    completionLatch.countDown();
                }
        );

        try {
            completionLatch.await();
        } catch (InterruptedException err) {
            throw new AssertionError("Unexpected Interruption", err);
        }

        // </QueryItems>
    }

    void deleteFamily(CosmosAsyncContainer container, VersionedJsonNode family, boolean failNow) {

        System.out.println("### DELETE");
        CosmosItemRequestOptions requestOptions = new CosmosItemRequestOptions();
        requestOptions.setIfMatchETag(family.getEtag());

        try {
            container.deleteItem(family.getJsonNode().get("id").asText(), new PartitionKey(family.getJsonNode().get("lastName").asText()), requestOptions).flatMap(itemResponse -> {
                System.out.println("   DELETE status" + itemResponse.getStatusCode());
                return Mono.just(itemResponse.getRequestCharge());
            }).block();
        } catch (CosmosException e) {
            System.out.println(" EXCEPTION " + e.toString());
            throw Exceptions.propagate(e);
        }
    }

    VersionedJsonNode readItem(CosmosAsyncContainer container, String id, String lastName) {
        System.out.println("### GET");
        return container.readItem(id, new PartitionKey(lastName), JsonNode.class).flatMap(res -> {
            System.out.println("   GET status " + res.getStatusCode());
            return Mono.just(new VersionedJsonNode(res.getItem(), res.getETag()));

        }).block();
    }

    private VersionedJsonNode createItem(CosmosAsyncContainer container, JsonNode item) {

        //  <CreateItem>
        System.out.println("### PUT (create)");
        return container.createItem(item,
                new PartitionKey(item.get("lastName").asText()), null).flatMap(res -> {
            System.out.println("   PUT status" + res.getStatusCode());
            return Mono.just(new VersionedJsonNode(res.getItem(), res.getETag()));
        }).block();
    }

    private VersionedJsonNode replaceItem(CosmosAsyncContainer container, VersionedJsonNode item) {

        //  <CreateItem>
        System.out.println("### PUT (replace)");
        CosmosItemRequestOptions reqOptions = new CosmosItemRequestOptions();
        reqOptions.setIfMatchETag(item.getEtag());
        return container.replaceItem(item.getJsonNode(), item.getJsonNode().get("id").asText(),
                new PartitionKey(item.getJsonNode().get("lastName").asText()), reqOptions).flatMap(res -> {
            System.out.println("   PUT status" + res.getStatusCode());
            return Mono.just(new VersionedJsonNode(res.getItem(), res.getETag()));
        }).block();
    }

}
