package com.snailmann.mongo.client;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 使用普通的Java客户端连接mongodb | 没有使用Spring Data Mongo方式
 */
public class MongoClient {

    public static void main(String[] args) {
        //连接mongo服务器，得到connection
        com.mongodb.MongoClient mongoClient = new com.mongodb.MongoClient("192.168.31.130", 27017);
        //通过connection获取具体的数据库
        MongoDatabase spitDB = mongoClient.getDatabase("spitdb");
/*        insert(spitDB, "spit");
        print(spitDB, "spit");*/
        query(spitDB,"spit");

        //关闭连接
        mongoClient.close();
    }

    /**
     * 向某个collection插入某个document
     *
     * @param mongoDB
     * @param collectionName
     */
    private static void insert(MongoDatabase mongoDB, String collectionName) {
        MongoCollection<Document> spit = mongoDB.getCollection(collectionName);
        String data = "{\n" +
                "\"content\": \"听说这个课程不错\",\n" +
                "\"userid\": \"1001\",\n" +
                "\"nickname\": \"小黑\",\n" +
                "\"visits\": 909\n" +
                "}";

        spit.insertOne(Document.parse(data));

    }


    /**
     * 插入测试数据
     * @param mongoDB
     * @param collectionName
     */
    private static void insertTestData(MongoDatabase mongoDB, String collectionName) {
        //测试数据的JSON集合
        List<String> jsonList = Stream.of(
                "{ \"_id\" : \"1\", \"content\" : \"我还是没有想明白到底为啥出 错\", \"userid\" : \"1012\", \"nickname\" : \"小明\", \"visits\" : 2020 }",
                "{ \"_id\" : \"2\", \"content\" : \"加班到半夜\", \"userid\" : \"1013\", \"nickname\" : \"凯 撒\", \"visits\" : 1023 }",
                "{ \"_id\" : \"3\", \"content\" : \"手机流量超了咋 办？\", \"userid\" : \"1013\", \"nickname\" : \"凯撒\", \"visits\" : 111 }",
                "{ \"_id\" : \"4\", \"content\" : \"坚持就是胜利\", \"userid\" : \"1014\", \"nickname\" : \"诺 诺\", \"visits\" : 1223 }"
        ).collect(Collectors.toList());
        //将测试数据转换成Document集合
        List<Document> documents = jsonList.stream().map(Document::parse).collect(Collectors.toList());
        //获取spit集合
        MongoCollection<Document> spit = mongoDB.getCollection(collectionName);
        //将测试数据插入spit集合中
        spit.insertMany(documents);
    }


    /**
     * 打印对应collection的所有document
     *
     * @param mongoDB
     * @param collectionName
     */
    private static void print(MongoDatabase mongoDB, String collectionName) {
        //得到要操作的集合| spit集合
        MongoCollection<Document> spit = mongoDB.getCollection(collectionName);
        FindIterable<Document> documents = spit.find();
        //遍历数据
        for (Document document : documents) {
            //直接输出
            System.out.println(document.toJson());
            System.out.println("content: " + document);
        }
    }

    /**
     * 条件查询
     * @param mongoDB
     * @param collectionName
     */
    private static void query(MongoDatabase mongoDB, String collectionName){
        //得到要操作的集合| spit集合
        MongoCollection<Document> spit = mongoDB.getCollection(collectionName);
        //封装查询条件
        BasicDBObject bson = new BasicDBObject("userid","1013");
        //查询spit集合中userid为1013的document
        FindIterable<Document> documents1 = spit.find(bson);
        documents1.forEach((Consumer<? super Document>) System.out::println);

        //查询spit集合中visists>=1000的所有document | find("{visits: {$gte: 1000}}")
        BasicDBObject bson2 = new BasicDBObject("visits",new BasicDBObject("$gte",1000d));
        //方法一，传统
        FindIterable<Document> documents2 = spit.find(bson2);
        //方法二，更简单直接,直接写bson语句就可以了
       /* FindIterable<Document> documents2 = spit.find(BasicDBObject.parse("{visits: {$gte: 1000}}"));*/
        documents2.forEach((Consumer<? super Document>) System.out::println);


    }
}
