/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.shenjitang.mongodbutils;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import java.util.HashMap;
import java.util.Map;
import net.sf.json.JSONSerializer;
import org.bson.BasicBSONCallback;
import org.bson.BsonDocument;
import org.bson.Document;

/**
 *
 * @author xiaolie
 */
public class QueryInfo {
    public String dbName;
    public String collName;
    public String action;
    public BsonDocument keys;
    public BsonDocument query;
    public Long limit;
    public Long skip;
    public BsonDocument order;
    public BsonDocument updateObj;
    
    public void setQuery(String json) {
        query = BsonDocument.parse(json);
        System.out.println(query);
    }
    
    public void setSort(String json) {
        order = BsonDocument.parse(json);
    }
    
    public String debugStr() {
        return "dbName:" + dbName + 
                "  collName:" + collName + 
                "  action:" + action + 
                " query:" + query + 
                "  limit:" + limit + 
                "  sort:" + order +
                "  updateObj:" + updateObj;
    }
}
