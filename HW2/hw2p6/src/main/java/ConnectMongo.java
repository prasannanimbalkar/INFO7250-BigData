/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */

/**
 *
 * @author prasannanimbalkar
 */
import java.io.File;
import java.io.FileNotFoundException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import javax.print.Doc;


public class ConnectMongo {


    public static List<Document> readLogFile() throws FileNotFoundException {
        
        List<Document> documentList = new ArrayList<>();
        File file = new File("/Users/prasannanimbalkar/Desktop/MSIS/BigData/HW2/access.log");

        Scanner scanner = new Scanner(file);
        scanner.useDelimiter("\n");
        System.out.println(scanner.next());
        
        while(scanner.hasNext()){
            String srcString = scanner.next();
            String[] srcArr = srcString.split(" ");
            String ipAddr = srcArr[0];

            String timeStamp = srcArr[3].replaceAll("\\[", "").replaceAll("\\]", "");
            String website = srcArr[6];
            System.out.println(ipAddr);
            System.out.println(timeStamp);
            System.out.println(website);

            Document newDoc = new Document();
            newDoc.append("ipAddress", ipAddr);
            newDoc.append("timeStamp", timeStamp);
            newDoc.append("website", website);
            System.out.println(newDoc);

            documentList.add(newDoc);

        }
        // closing the scanner stream
        scanner.close();
        return documentList;
    }

    
    public static void main(String[] args) throws FileNotFoundException, UnknownHostException {
        MongoClient mongo = MongoClients.create();
        MongoDatabase db = mongo.getDatabase("HW2-P6");
        MongoCollection<Document> accessCollection = db.getCollection("access");

        if (accessCollection.countDocuments() == 0) {
            List<Document> listOfDoc = readLogFile();
            accessCollection.insertMany(listOfDoc);
            System.out.println("Document counts " + accessCollection.countDocuments());

        }

    }
}

