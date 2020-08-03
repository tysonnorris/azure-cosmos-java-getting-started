package com.azure.cosmos.sample.common;

import com.fasterxml.jackson.databind.JsonNode;

public class VersionedJsonNode {
    public VersionedJsonNode(JsonNode jsonNode, String etag){
        this.jsonNode = jsonNode;
        this.etag = etag;
    }
    JsonNode jsonNode;
    String etag;

    public String getEtag() { return etag;}
    public JsonNode getJsonNode() { return jsonNode;}

}
