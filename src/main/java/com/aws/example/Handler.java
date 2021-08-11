package com.aws.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

/**
*  This is the entry point for the Lambda function
*/

public class Handler {

 public Void handleRequest(Context context) {
    LambdaLogger logger = context.getLogger();
    logger.log("Got logger from context");
    ScanEmployees scanEmployees = new ScanEmployees(logger);
   Boolean ans =  scanEmployees.sendEmployeMessage();
    if (ans)
        logger.log("Messages sent: " + ans);
    return null;
}
}