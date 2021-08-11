package com.aws.example;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.SnsException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import com.amazonaws.services.lambda.runtime.LambdaLogger;

/*
Sends a text message to any employee that reached the one year anniversary mark
*/

public class ScanEmployees {
	
	LambdaLogger logger=null;

	public ScanEmployees(LambdaLogger Llogger)
	{
		logger=Llogger;
	}

  public Boolean sendEmployeMessage() {

    Boolean send = false;
    String myDate = getDate();

   Region region = Region.US_EAST_1;
   DynamoDbClient ddb = DynamoDbClient.builder()
           .region(region)
           .build();
   
   logger.log("Got DynamoDbClient object");

   // Create a DynamoDbEnhancedClient and use the DynamoDbClient object
   DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
           .dynamoDbClient(ddb)
           .build();
   
   logger.log("Got Enhanced DynamoDbClient object");

   // Create a DynamoDbTable object based on Employee
   DynamoDbTable<Employee> table = enhancedClient.table("Employee", TableSchema.fromBean(Employee.class));
   
   logger.log("Got Employee table object");

   try {
       AttributeValue attVal = AttributeValue.builder()
           .s(myDate)
           .build();
       
       logger.log("Built query by date:"+myDate);

       // Get only items in the Employee table that match the date
       Map<String, AttributeValue> myMap = new HashMap<>();
       myMap.put(":val1", attVal);

       Map<String, String> myExMap = new HashMap<>();
       myExMap.put("#startDate", "startDate");

       Expression expression = Expression.builder()
           .expressionValues(myMap)
           .expressionNames(myExMap)
           .expression("#startDate = :val1")
           .build();

       logger.log("Got expression object");
       
       ScanEnhancedRequest enhancedRequest = ScanEnhancedRequest.builder()
           .filterExpression(expression)
           .limit(15) // you can increase this value
           .build();
       
       logger.log("Got enhancedRequest object");

       // Get items in the Employee table
       Iterator<Employee> employees = table.scan(enhancedRequest).items().iterator();
       
       logger.log("Got employees Iterator object");

       while (employees.hasNext()) {
           Employee employee = employees.next();
           
           String first = employee.getFirst();
           String phone = employee.getPhone();

           logger.log("Sending text msg to "+first+" "+ phone);
           // Send an anniversary message!
           sentTextMessage(first, phone);
           logger.log("Sent text msg to "+first+" "+ phone);
           
           send = true;
       }
       
       
   } catch (DynamoDbException e) {
	   logger.log("GotDynamoDbException:"+e.getMessage());
       System.err.println(e.getMessage());
       System.exit(1);
   }
   return send;
 }


//Use the Amazon SNS Service to send a text message
private void sentTextMessage(String first, String phone) {

   SnsClient snsClient = SnsClient.builder()
           .region(Region.US_WEST_2)
           .build();
   logger.log("Built snsClient object");
   String message = first +" happy one year anniversary. We are very happy that you have been working here for a year! ";

   try {
       PublishRequest request = PublishRequest.builder()
               .message(message)
               .phoneNumber(phone)
               .build();
       logger.log("Built PublishRequest object");
       snsClient.publish(request);
       logger.log("Sent request to SNS");
   } catch (SnsException e) {
       System.err.println(e.awsErrorDetails().errorMessage());
       System.exit(1);
   }
}

public String getDate() {

   String DATE_FORMAT = "yyyy-MM-dd";
   DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
   DateTimeFormatter dateFormat8 = DateTimeFormatter.ofPattern(DATE_FORMAT);

   Date currentDate = new Date();
   System.out.println("date : " + dateFormat.format(currentDate));
   LocalDateTime localDateTime = currentDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
   System.out.println("localDateTime : " + dateFormat8.format(localDateTime));

   localDateTime = localDateTime.minusYears(1);
   String ann = dateFormat8.format(localDateTime);
   return ann;
}
}