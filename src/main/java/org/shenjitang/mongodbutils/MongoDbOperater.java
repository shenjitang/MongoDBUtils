/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.shenjitang.mongodbutils;

import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.model.Filters;
import java.io.ByteArrayInputStream;
import org.shenjitang.beanutils.BeanUtilEx;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Pattern;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.DateConverter;
import org.apache.commons.beanutils.converters.IntegerConverter;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

/**
 * @author xiaolie
 */
public class MongoDbOperater {

    public MongoDbOperater() {
    }
    
    public MongoDbOperater(String address) {
        mongoClient = MongoClients.create(address);
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public void setMongoClient(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    private MongoClient mongoClient;

    public void insert(String dbName, String colName, Map map) {
        MongoDatabase db = mongoClient.getDatabase(dbName);
        MongoCollection coll = db.getCollection(colName);
        Document doc = new Document(map);
        coll.insertOne(doc);
    }

    public void insert(String dbName, String colName, Object obj) throws Exception {
        insert(dbName, colName, BeanUtilEx.transBean2Map(obj));
    }

    public void insert(String dbName, String colName, String json) {
        MongoDatabase db = mongoClient.getDatabase(dbName);
        MongoCollection coll = db.getCollection(colName);
        BsonDocument doc = BsonDocument.parse(json);
        coll.insertOne(doc);
    }

    public void update(String dbName, String colName, Map findMap, Map recordMap) throws Exception {
        MongoDatabase db = mongoClient.getDatabase(dbName);
        MongoCollection coll = db.getCollection(colName);
        coll.updateMany(map2Bson(findMap), new Document("$set", new Document(recordMap)));
    }

    public void update(String dbName, String colName, Bson findMap, Map recordMap) throws Exception {
        MongoDatabase db = mongoClient.getDatabase(dbName);
        MongoCollection coll = db.getCollection(colName);
        coll.updateMany(findMap, new Document("$set", new Document(recordMap)));
    }

    public void update(String dbName, String colName, Bson findMap, Object obj) throws Exception {
        MongoDatabase db = mongoClient.getDatabase(dbName);
        MongoCollection coll = db.getCollection(colName);
        coll.updateMany(findMap, new Document("$set", new Document(BeanUtilEx.transBean2Map(obj))));
    }

    public void update(String dbName, String colName, String findJson, String json) {
        MongoDatabase db = mongoClient.getDatabase(dbName);
        MongoCollection coll = db.getCollection(colName);
        coll.updateMany(BsonDocument.parse(findJson), new Document("$set", BsonDocument.parse(json)));
    }

    public void update(String dbName, String sql, Object obj) throws Exception {
        QueryInfo query = sql2QueryInfo(dbName, sql);
        update(dbName, query.collName, query.query, obj);
    }

    public void update(String dbName, String sql) throws Exception {
        QueryInfo query = sql2QueryInfo(dbName, sql);
        update(dbName, query.collName, query.query, new Document("$set", query.updateObj));
    }

    public void remove(String dbName, String sql) throws JSQLParserException {
        QueryInfo query = sql2QueryInfo(dbName, sql);
        MongoDatabase db = mongoClient.getDatabase(dbName);
        MongoCollection coll = db.getCollection(query.collName);
        coll.deleteMany(query.query);
    }

    public void remove(String dbName, String colName, Map query) {
        MongoDatabase db = mongoClient.getDatabase(dbName);
        MongoCollection coll = db.getCollection(colName);
        coll.deleteMany(new Document(query));
    }

    public List find(String dbName, String sql) throws JSQLParserException {
        QueryInfo query = sql2QueryInfo(dbName, sql);
        return find(query);
    }

    public Document findOne(String dbName, String sql) throws JSQLParserException {
        QueryInfo query = sql2QueryInfo(dbName, sql);
        return findOne(query);
    }
    
    public Document get(String dbName, String collName, String hexId) {
        MongoDatabase db = mongoClient.getDatabase(dbName);
        MongoCollection coll = db.getCollection(collName);
        ObjectId id = new ObjectId(hexId);
        return (Document)coll.find(Filters.eq("_id", id)).first();
    }

    public <T> T findOneObj(String dbName, String sql, Class<T> clazz) throws JSQLParserException, InstantiationException, IllegalAccessException, InvocationTargetException {
        Map map = findOne(dbName, sql);
        if (map == null) {
            return null;
        }
        T obj = clazz.newInstance();
        ConvertUtils.register(new DateConverter(null), Date.class);
        ConvertUtils.register(new IntegerConverter(null), Integer.class);
        BeanUtils.populate(obj, map);
        return obj;
    }

    public Document findOne(QueryInfo queryInfo) {
        MongoDatabase db = mongoClient.getDatabase(queryInfo.dbName);
        MongoCollection coll = db.getCollection(queryInfo.collName);
        Document cur = (Document)coll.find(queryInfo.query).first();
        return cur;
    }

    public List<Document> find(QueryInfo queryInfo) {
        MongoDatabase db = mongoClient.getDatabase(queryInfo.dbName);
        MongoCollection coll = db.getCollection(queryInfo.collName);
        FindIterable cursor = null;
        if (queryInfo.query != null) {
            cursor = coll.find(queryInfo.query);
        } else {
            cursor = coll.find();
        }
        if (queryInfo.order != null) {
            cursor = cursor.sort(queryInfo.order);
        }
        if (queryInfo.limit != null) {
            cursor.limit(queryInfo.limit.intValue());
        }
        if (queryInfo.skip != null) {
            cursor.skip(queryInfo.skip.intValue());
        }
        return cursor2list(cursor);
    }

    public List find(String dbname, String collName, Map queryMap, int start, int limit) {
        MongoDatabase db = mongoClient.getDatabase(dbname);
        MongoCollection coll = db.getCollection(collName);
        Document query = new Document(queryMap);
        return find(coll, query, start, limit);
    }

    public List find(String dbname, String collName, Map queryMap, Map orderMap, int start, int limit) {
        MongoDatabase db = mongoClient.getDatabase(dbname);
        MongoCollection coll = db.getCollection(collName);
        Document query = new Document(queryMap);
        Document order = new Document(orderMap);
        return find(coll, query, order, start, limit);
    }
    
    public List find(String dbname, String collName, Map queryMap) {
        MongoDatabase db = mongoClient.getDatabase(dbname);
        MongoCollection coll = db.getCollection(collName);
        Document query = new Document(queryMap);
        return find(coll, query);
    }

    public List<Document> findAll(String dbname, String collName) {
        MongoDatabase db = mongoClient.getDatabase(dbname);
        MongoCollection coll = db.getCollection(collName);
        return find(coll, null);
    }

    public long count(String dbname, String collName, Map queryMap) {
        MongoDatabase db = mongoClient.getDatabase(dbname);
        MongoCollection coll = db.getCollection(collName);
        long count;
        if (queryMap == null) {
            count = coll.count();
        } else {
            Document query = new Document(queryMap);
            count = coll.count(query);
        }
        return count;
    }

    public long count(String dbname, String collName) {
        return count(dbname, collName, null);
    }

    public <T> T findOneObj(String dbname, String collName, Map queryMap, Class<T> clazz) throws InstantiationException, IllegalAccessException, InvocationTargetException {
        MongoDatabase db = mongoClient.getDatabase(dbname);
        MongoCollection coll = db.getCollection(collName);
        Document query = new Document(queryMap);
        return (T)coll.find( query, clazz).first();
    }

    public <T> T findOneObj(String dbname, String collName, Bson queryMap, Class<T> clazz) throws InstantiationException, IllegalAccessException, InvocationTargetException {
        MongoDatabase db = mongoClient.getDatabase(dbname);
        MongoCollection coll = db.getCollection(collName);
        return (T)coll.find(queryMap, clazz).first();
    }

    public Map findOne(String dbname, String collName, Map queryMap) throws InstantiationException, IllegalAccessException, InvocationTargetException {
        MongoDatabase db = mongoClient.getDatabase(dbname);
        MongoCollection coll = db.getCollection(collName);
        Document query = new Document(queryMap);
        return (Map)coll.find( query, Map.class).first();
    }

    public List<Document> find(MongoCollection coll, Document query) {
        FindIterable cursor = null;
        if (query == null) {
            cursor = coll.find();
        } else {
            cursor = coll.find(query);
        }
        return cursor2list(cursor);
    }

    public List<Document> find(MongoCollection coll, Document query, int start, int limit) {
        FindIterable cursor = null;
        if (query == null) {
            cursor = coll.find();
        } else if (start == 0) {
            cursor = coll.find(query).limit(limit);
        } else {
            cursor = coll.find(query).skip(start).limit(limit);
        }
        return cursor2list(cursor);
    }

    public List<Document> find(MongoCollection coll, Document query, Document order, int start, int limit) {
        FindIterable cursor = null;
        if (query == null) {
            cursor = coll.find();
        } else {
            cursor = coll.find(query);
        }
        if (order != null) cursor = cursor.sort(order);

        if (start != 0 && limit != 0) {
            cursor.skip(start).limit(limit);
        }
        if (start == 0 && limit != 0) {
            cursor.limit(limit);
        }

        return cursor2list(cursor);
    }

    /*
    public Map group(String dbName, String collName, Map keys, Map cond, Map initial, String reduce) {
        DB db = mongoClient.getDB(dbName);
        DBCollection coll = db.getCollection(collName);

        if (initial == null || initial.isEmpty()) {
            initial = new HashMap();
            initial.put("count", 0);
        }
        if (StringUtils.isBlank(reduce)) {
            reduce = "function(obj,out) {}";
        }
        return group(coll, new BasicDBObject(keys), new BasicDBObject(cond), new BasicDBObject(initial), reduce);
    }

    public Map group(DBCollection coll, BasicDBObject keys, BasicDBObject cond, BasicDBObject initial, String reduce) {
        DBObject obj = coll.group(keys, cond, initial, reduce);
        return obj.toMap();
    }
    */


    private List<Document> cursor2list(FindIterable cursor) {
        final List<Document> list = new ArrayList();
        cursor.forEach(new Block() {
            @Override
            public void apply(Object t) {
                list.add((Document)t);
            }
        });
        return list;
    }

    public static Pattern dateP = Pattern.compile("^[12][09][0-9][0-9]\\-[0-9][0-9]\\-[0-9][0-9]$");
    public static Pattern timeP = Pattern.compile("^[12][09][0-9][0-9]\\-[0-9][0-9]\\-[0-9][0-9]\\s[0-9][0-9]\\:[0-9][0-9]\\:[0-9][0-9]$");

    public QueryInfo sql2QueryInfo(String dbName, String sql) throws JSQLParserException {
        QueryInfo queryInfo = new QueryInfo();
        queryInfo.dbName = dbName;
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        Statement statement = parserManager.parse(new StringReader(sql));
        Expression whereExpression = null;
        List<OrderByElement> orderList = null;
        if (statement instanceof Select) {
            queryInfo.action = "select";
            Select select = (Select) statement;
            PlainSelect selectBody = (PlainSelect) select.getSelectBody();
            whereExpression = selectBody.getWhere();
            queryInfo.collName = selectBody.getFromItem().toString();
            orderList = selectBody.getOrderByElements();
            Limit limit = selectBody.getLimit();
            //Long limit = selectBody.getLimit().getRowCount();
            if (null != limit) {
                Expression offset = limit.getOffset();
                Expression rowCount = limit.getRowCount();
                if (offset != null && offset instanceof LongValue) {
                    queryInfo.skip = ((LongValue)(offset)).getValue();
                }
                if (rowCount != null && rowCount instanceof LongValue) {
                    queryInfo.limit = ((LongValue)(rowCount)).getValue();
                }
            }
        } else if (statement instanceof Delete) {
            queryInfo.action = "delete";
            Delete delete = (Delete) statement;
            whereExpression = delete.getWhere();
            queryInfo.collName = delete.getTable().getName();
        } else if (statement instanceof Update) {
            //throw new RuntimeException("update 暂时不支持");
            queryInfo.action = "update";
            Update update = (Update) statement;
            whereExpression = update.getWhere();
            queryInfo.collName = update.getTables().get(0).getName();
            List<Column> columnList = update.getColumns();
            List<Expression> expressionList = update.getExpressions();
            queryInfo.updateObj = new BsonDocument();
            for (int i = 0; i < columnList.size(); i++) {
                String v = expressionList.get(i).toString();
                String columnName = columnList.get(i).getColumnName();
                if (v.startsWith("'") && v.endsWith("'")) {
                    String v1 = v.substring(1, v.length() - 1);
                    try {
                        if (dateP.matcher(v1).find()) {
                            BsonDateTime value = new BsonDateTime(DateUtils.parseDate(v1, "yyyy-MM-dd").getTime());
                            queryInfo.updateObj.put(columnList.get(i).getColumnName(), value);
                        } else if (timeP.matcher(v1).find()) {
                            BsonDateTime value = new BsonDateTime(DateUtils.parseDate(v1, "yyyy-MM-dd HH:mm:ss").getTime());
                            queryInfo.updateObj.put(columnList.get(i).getColumnName(), value);
                        } else {
                            BsonString value = new BsonString(v1);
                            queryInfo.updateObj.put(columnList.get(i).getColumnName(), value);
                        }
                    } catch (ParseException e) {
                        throw new JSQLParserException("不正确的日期格式：yyyy-MM-dd 或 yyyy-MM-dd HH:mm:ss value:" + v1, e);
                    }
                } else {
                    if (v.trim().equalsIgnoreCase("true") || v.trim().equalsIgnoreCase("false")) {
                        BsonBoolean value = new BsonBoolean(BooleanUtils.toBooleanObject(v.trim()));
                        queryInfo.updateObj.put(columnName, value);
                    } else if (v.contains(".")) {
                        try {
                            Double dv = Double.valueOf(v);
                            BsonDouble value = new BsonDouble(dv);
                            queryInfo.updateObj.put(columnName, value);
                        } catch (Exception ee) {
                            queryInfo.updateObj.put(columnName, new BsonString(v));
                        }
                    } else if (NumberUtils.isDigits(v)) {
                        try {
                            Integer iv = Integer.valueOf(v.trim());
                            BsonInt32 value = new BsonInt32(iv);
                            queryInfo.updateObj.put(columnName, value);
                        } catch (Exception e) {
                            queryInfo.updateObj.put(columnName, new BsonString(v));
                        }
                    } else {
                        queryInfo.updateObj.put(columnName, new BsonString(v));
                    }
                }
            }
        } else if (statement instanceof Insert) {
            throw new RuntimeException("insert 不支持");
        } else {
            throw new JSQLParserException("不支持的sql语句:" + sql);
        }
        if (whereExpression != null) {
            Sql2MongoExpressVisitor visitor = new Sql2MongoExpressVisitor();
            whereExpression.accept(visitor);
            queryInfo.query = visitor.getQuery();
        }
        if (orderList != null) {
            queryInfo.order = new BsonDocument();
            for (OrderByElement ele : orderList) {
                queryInfo.order.put(ele.getExpression().toString(), new BsonInt32(ele.isAsc() ? 1 : -1));
            }
        }


//        System.out.println(queryInfo.debugStr());
        return queryInfo;
    }

    /**
     * 上传文件到mongodb
     *
     * @param dbName     数据库名称
     * @param fsName     GridFS名称，缺省是fs，如果是null，就用缺省的。
     * @param file       要上传的文件。
     * @param fsFileName 在GridFS中的文件名，如果是null，就是file的名称（不带路径）。
     * @throws FileNotFoundException
     * @throws IOException
     */
    public ObjectId upload2GridFS(String dbName, File file, String fsFileName) throws FileNotFoundException, IOException {
        if (fsFileName == null) {
            fsFileName = file.getName();
        }
        InputStream input = new FileInputStream(file);
        ObjectId id = upload2GridFS(dbName, input, fsFileName);
        input.close();
        return id;
    }

    /**
     * 上传文件到mongodb
     *
     * @param dbname
     * @param dbName     数据库名称
     * @param fsName     GridFS名称，缺省是fs，如果是null，就用缺省的。
     * @param input      文件数据流
     * @param fsFileName 在GridFS中的文件名
     * @return 
     * @throws FileNotFoundException
     * @throws IOException
     */
    public ObjectId upload2GridFS(String dbname, InputStream input, String fsFileName) throws FileNotFoundException, IOException {
        MongoDatabase db = mongoClient.getDatabase(dbname);
        GridFSBucket bucket = GridFSBuckets.create(db);
        ObjectId fileId = bucket.uploadFromStream(fsFileName, input);
        return fileId;
    }

    public ObjectId upload2GridFS(String dbName, byte[] bytes, String fsFileName) throws FileNotFoundException, IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        return upload2GridFS(dbName, in, fsFileName);
    }

    public ObjectId upload2GridFS(String dbName, String content, String fsFileName, String encoding) throws FileNotFoundException, IOException {
        return upload2GridFS(dbName, content.getBytes(encoding), fsFileName);
    }

    /**
     * 从GridFS中下载文件
     *
     * @param dbName     数据库名称
     * @param fsName     GridFS名称，缺省是fs，如果是null，就用缺省的。
     * @param fsFileName 在GridFS中的文件名
     * @param fileName   保存下载的文件名，如果为空，就以fsFileName作为文件名。如果是目录，就以fsFileName作为文件名放到此目录中
     * @return 下载下来的本地文件名
     * @throws FileNotFoundException
     * @throws IOException
     */
    public File downloadFsFile(String dbname, String fsFileName, String fileName) throws FileNotFoundException, IOException {
        File outFile = new File(fileName);
        FileOutputStream out = new FileOutputStream(outFile);
        GridFSBucket bucket = GridFSBuckets.create(mongoClient.getDatabase(dbname));
        bucket.downloadToStream(fsFileName, out);
        return outFile;
    }

    /**
     * 从GridFS中删除文件
     *
     * @param dbName   数据库名称
     * @param fileName   
     * @param queryMap GridFS文件的检索条件
     */
    public void removeFsFile(String dbname, String fileName, Map queryMap) {
        GridFSBucket bucket = GridFSBuckets.create(mongoClient.getDatabase(dbname));
        MongoCursor<GridFSFile> cursor = bucket.find(Filters.eq("filename", fileName)).iterator();
        while(cursor.hasNext()) {
            GridFSFile fileInfo = cursor.next();
            bucket.delete(fileInfo.getObjectId());
        }
    }


    public static void main(String[] args) throws Exception {
        String dbname = "test";
        String collname = "collection1";
        MongoDbOperater operater = new MongoDbOperater("mongodb://xl:最常用@shenjimongo-shard-00-00-duqjb.mongodb.net:27017,shenjimongo-shard-00-01-duqjb.mongodb.net:27017,shenjimongo-shard-00-02-duqjb.mongodb.net:27017/test?ssl=true&replicaSet=ShenjiMongo-shard-0&authSource=admin&retryWrites=true");
        /*
        operater.remove("test", "delete from collection1 where anme='秦小'");
        Map record = new HashMap();
        record.put("name", "大卫");
        record.put("age", 22);
        operater.insert("test", "collection1", record);
        */
        List<Document> list = operater.findAll(dbname, collname);
        print(list);
        //Document map = operater.findOne(dbname, "select * from collection1 where name='秦小'");
        //print(map);
        System.out.println("=");
        print(operater.get(dbname, collname, "5b87779262e5f57e8f08dd35"));
        //String sql = "select * from collection1 where name='秦小' and age between 15 and 17 limit 3";
        //String sql = "select * from collection1 where age between 15 and 17 and name='张三' limit 2";
        String sql = "select * from collection1 where age > 15 and age < 20 and (name='张三' or name='大卫') limit 2";
        //String sql = "select * from collection1 where name='张三' or name='秦小' limit 2";
        list = operater.find(dbname, sql);
        System.out.println("===");
        print(list);
    }
    
    private static void print(Document rec) {
        ObjectId id = rec.getObjectId("_id");
        System.out.println(id.toHexString() + " name:" + rec.get("name") + " age:" + rec.get("age"));
    }
    
    private static void print(List<Document> list) {
        for (Document rec : list) {
            print(rec);
        }
    }

    private Bson map2Bson(Map findMap) {
        Bson result = null;
        for (Object key : findMap.keySet()) {
            if (result == null) {
                result = Filters.eq(key.toString(), findMap.get(key));
            } else {
                result = Filters.and(result, Filters.eq(key.toString(), findMap.get(key)));
            }
        }
        return result;
    }


}
