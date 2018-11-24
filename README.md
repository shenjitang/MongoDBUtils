# MongoDBUtils
MongoDBUtils is java client api with SQL support.
# Quick Start
- create
```
MongoDbOperater operater = new MongoDbOperater("mongodb://user:pwd@xxxmongo-shard-00-00-duqjb.mongodb.net:27017/db");

```

- sql support
```
List list = operater.find("testdb", "select * from collection_007");
operater.update("testDb", "update user set mobileNum='13388876541'");
operater.insert("testDb", "insert into user (name, dep_id) values ("Tom", 2);

```

- entity class mapping support
```
User user = operater.get(User.class, "testDb", "user", userId);
List<User> users = operater.find(User.class, "testdb", "select * from user where city='shanghai' and dep_id in (1,4,6,9)");
operate.insert("testdb", "user", user);
operate.update("testdb", "user", user);
```

- MongoDb api direct invoke (drop table)
```
mongoDbOperation.getMongoClient().getDatabase("testdb").getCollection("collection_007").drop();
```

- spring 
```
    <bean id="mongoDbOperater" class="org.shenjitang.mongodbutils.MongoDbOperater">
        <constructor-arg>
            <value>${mongo.address}</value>
        </constructor-arg>
    </bean>
```