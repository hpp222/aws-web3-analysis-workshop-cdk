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
        
        VpcStack vpcStack                              = new VpcStack(app, "VpcStack", stackProps);
        RedshiftServerlessStack redshiftStack          = new RedshiftServerlessStack(app, "RedshiftServerlessStack", stackProps, vpcStack.getVpc());
        EthereumStreamingStack ethereumStreamingStack  = new EthereumStreamingStack(app, "EthereumStreamingStack", stackProps, vpcStack.getVpc(), redshiftStack);
        BitcoinStreamingStack bitcoinStreamingStack    = new BitcoinStreamingStack(app, "BitcoinStreamingStack", stackProps, vpcStack.getVpc(), redshiftStack);
        IoTexStreamingStack ioTexStreamingStack        = new IoTexStreamingStack(app, "IoTexStreamingStack", stackProps, vpcStack.getVpc(), redshiftStack);
        PolygonStreamingStack polygonStreamingStack    = new PolygonStreamingStack(app, "PolygonStreamingStack", stackProps, vpcStack.getVpc(), redshiftStack);
        //QuicksightStack quicksightStack                = new QuicksightStack(app, "QuicksightStack", stackProps, vpcStack.getVpc(), redshiftStack);
        
        redshiftStack.getNode().addDependency(vpcStack);
        ethereumStreamingStack.getNode().addDependency(redshiftStack);
        bitcoinStreamingStack.getNode().addDependency(redshiftStack);
        ioTexStreamingStack.getNode().addDependency(redshiftStack);
        polygonStreamingStack.getNode().addDependency(redshiftStack);

        

        app.synth();
    }
}

