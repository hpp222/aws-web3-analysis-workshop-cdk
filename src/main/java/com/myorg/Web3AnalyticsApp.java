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
        
        VpcStack vpcStack                     = new VpcStack(app, "VpcStack", stackProps);
        StreamingIngestionStack streamStack   = new StreamingIngestionStack(app, "StreamingIngestionStack", stackProps);
        EC2Stack EC2Stack                     = new EC2Stack(app, "EC2Stack", stackProps, vpcStack.getVpc());
        RedshiftServerlessStack redshiftStack = new RedshiftServerlessStack(app, "RedshiftServerlessStack", stackProps, vpcStack.getVpc());
        redshiftStack.getNode().addDependency(streamStack);

        app.synth();
    }
}

