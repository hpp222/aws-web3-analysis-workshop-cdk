package com.myorg;

import software.amazon.awscdk.App;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.StackProps;

public class Web3AnalyticsApp {
    public static void main(final String[] args) {
        App app = new App();
        
        StackProps stackProps = new StackProps.Builder()
            .env(
                Environment.builder()
                    .region(System.getenv("CDK_DEFAULT_REGION"))  
                    .account(System.getenv("CDK_DEFAULT_ACCOUNT"))
                    .build())
            .build();
        
        Web3AnalyticsStack stack = new Web3AnalyticsStack(app, "Web3AnalyticsStack", stackProps);
        
        app.synth();
    }
}

