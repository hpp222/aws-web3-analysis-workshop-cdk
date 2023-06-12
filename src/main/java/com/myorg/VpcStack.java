package com.myorg;

import software.constructs.Construct;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.ec2.*;
import java.util.List;
import software.amazon.awscdk.CfnOutput;

public class VpcStack extends Stack {
    
    public final Vpc vpc;
    
    public VpcStack(final Construct scope, final String id) {
        this(scope, id, null);
    }    
    
    public VpcStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);    
        
        final String  vpcName = "Web3VPC";
        int   subnetCidrMask  = 24;
        int   maxAzs          = 3;
        
        vpc = Vpc.Builder.create(this, "Web3Vpc")
                .enableDnsHostnames(true)
                .enableDnsSupport(true)
                .maxAzs(maxAzs)
                .subnetConfiguration(List.of(SubnetConfiguration.builder()
                    .name("Public")
                    .cidrMask(subnetCidrMask)
                    .subnetType(SubnetType.PUBLIC)
                    .mapPublicIpOnLaunch(true)
                    .build()))
                .vpcName(vpcName)    
                .build();  
        //CfnOutput.Builder.create(this, "VpcId").value(vpc.getVpcId()).build();
    }
    
    public Vpc getVpc() {
        return vpc;
    }

    
}