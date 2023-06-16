package com.myorg;

import software.constructs.Construct;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.ec2.*;
import software.amazon.awscdk.services.iam.*;
import java.util.*;
import software.amazon.awscdk.CfnParameter;

public class EC2Stack extends Stack {
    

    public EC2Stack(final Construct scope, final String id) {
        this(scope, id, null, null);
    }    
    
    public EC2Stack(final Construct scope, final String id, final StackProps props, Vpc vpc) {
        super(scope, id, props);  
        
        final CfnParameter imageID = CfnParameter.Builder.create(this, "ImageID")
                .type("String")
                .description("The ID of the ethereum full node image")
                .build();
        
        final String REGION   = System.getenv("CDK_DEFAULT_REGION");
        
        // create a security group for Ethereum instance
        final SecurityGroup sg = SecurityGroup.Builder.create(this, "Ethereum-SG")
                .vpc(vpc)
                .allowAllOutbound(true)
                .securityGroupName("Ethereum-SG")
                .build();
        sg.addIngressRule(Peer.anyIpv4(), Port.tcp(22), "allow ssh access from anywhere");
        sg.addIngressRule(Peer.anyIpv4(), Port.tcp(30303), "Geth runs on port 30303 for external listening");  
        sg.addIngressRule(Peer.anyIpv4(), Port.tcp(9000), "Lighthouse, by default, uses port 9000 for both TCP and UDP"); 
        
        // crete a key pair
        final CfnKeyPair cfnKeyPair = CfnKeyPair.Builder.create(this, "Web3KeyPair")
                .keyName("Web3KeyPair")
                .build();
        
        
        // create instance from Ethereum full node image
        Instance.Builder.create(this, "Eth-Instance")
                .instanceType(InstanceType.of(InstanceClass.M6G, InstanceSize.XLARGE2))
                .machineImage(new GenericLinuxImage(Map.of(REGION, imageID.getValueAsString())))
                .vpc(vpc)
                .keyName("Web3KeyPair")
                .securityGroup(sg)
                .vpcSubnets(SubnetSelection.builder().subnetType(SubnetType.PUBLIC).build())
                .build();
        
    }
    
}