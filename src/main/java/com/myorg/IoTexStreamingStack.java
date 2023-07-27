package com.myorg;

import software.constructs.Construct;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.ec2.*;
import software.amazon.awscdk.services.iam.*;
import java.util.*;
import software.amazon.awscdk.CfnParameter;
import software.amazon.awscdk.services.kinesis.*;
import software.amazon.awscdk.services.lambda.*;
import software.amazon.awscdk.services.lambda.eventsources.*;
import software.amazon.awscdk.services.redshiftserverless.*;
import software.amazon.awscdk.customresources.*;
import software.amazon.awscdk.services.secretsmanager.*;

public class IoTexStreamingStack extends Stack {
    final String REGION  = System.getenv("CDK_DEFAULT_REGION");
    
    public IoTexStreamingStack(final Construct scope, final String id) {
        this(scope, id, null, null, null);
    }    
    
    public IoTexStreamingStack(final Construct scope, final String id, final StackProps props, Vpc vpc, RedshiftServerlessStack redshfit) {
        super(scope, id, props);  
        
        final CfnParameter imageID = CfnParameter.Builder.create(this, "IoTexImageID")
                .type("String")
                .description("The ID of the IoTex full node image")
                .build();
        
        final SecurityGroup sg = SecurityGroup.Builder.create(this, "IoTex-SG")
                .vpc(vpc)
                .allowAllOutbound(true)
                .securityGroupName("IoTex-SG")
                .build();
        sg.addIngressRule(Peer.anyIpv4(), Port.tcp(22), "allow ssh access from anywhere");
        sg.addIngressRule(Peer.anyIpv4(), Port.tcp(8080), "For health monitoring");  
        sg.addIngressRule(Peer.anyIpv6(), Port.tcp(8080), "For health monitoring");  
        sg.addIngressRule(Peer.anyIpv4(), Port.tcp(4689), "For Blockchain syncing");  
        sg.addIngressRule(Peer.anyIpv6(), Port.tcp(4689), "For Blockchain syncing"); 
        sg.addIngressRule(Peer.anyIpv4(), Port.tcp(14014), "For incoming API requests (required if you run a Native IoTeX API Gateway)"); 
        sg.addIngressRule(Peer.anyIpv6(), Port.tcp(14014), "For incoming API requests (required if you run a Native IoTeX API Gateway)"); 
        sg.addIngressRule(Peer.anyIpv4(), Port.tcp(15015), "For incoming API requests (needed if you want to allow Ethereum API requests)"); 
        sg.addIngressRule(Peer.anyIpv6(), Port.tcp(15015), "For incoming API requests (needed if you want to allow Ethereum API requests)"); 
        sg.addIngressRule(Peer.anyIpv4(), Port.tcp(80), "For http traffic");  
        sg.addIngressRule(Peer.anyIpv6(), Port.tcp(80), "For http traffic"); 
        
        final CfnKeyPair cfnKeyPair = CfnKeyPair.Builder.create(this, "IoTexKeyPair")
                .keyName("IoTexKeyPair")
                .build();
        
        final Role instanceRole = Role.Builder.create(this, "IoTexInstanceRole")
                .assumedBy(new ServicePrincipal("ec2.amazonaws.com"))
                .roleName("IoTexInstanceRole")
                .build(); 
        instanceRole.addToPolicy(PolicyStatement.Builder.create()
                .actions(List.of("kinesis:PutRecord","kinesis:PutRecords"))
                .effect(Effect.ALLOW)
                .resources(List.of("*"))
                .build());    
                
        Instance.Builder.create(this, "IoTex-Full-Node")
                .instanceType(InstanceType.of(InstanceClass.R5, InstanceSize.LARGE))
                .machineImage(new GenericLinuxImage(Map.of(REGION, imageID.getValueAsString())))
                .vpc(vpc)
                .keyName(cfnKeyPair.getKeyName())
                .role(instanceRole)
                .securityGroup(sg)
                .vpcSubnets(SubnetSelection.builder().subnetType(SubnetType.PUBLIC).build())
                .build();

        final Stream rawStream = Stream.Builder.create(this, "raw_iotex_stream")
                .streamName("raw_iotex_stream")
                .build();
                
        final Stream processedSteram = Stream.Builder.create(this, "processed_iotex_stream")
                .streamName("processed_iotex_stream")
                .build();     
        
        final Role lambdaRole = Role.Builder.create(this, "IoTexLambdaStreamingRole")
                .assumedBy(new ServicePrincipal("lambda.amazonaws.com"))
                .roleName("IoTexLambdaStreamingRole")
                .build();
        lambdaRole.addToPolicy(PolicyStatement.Builder.create()
                .actions(List.of("kinesis:PutRecords","kinesis:PutRecord"))
                .effect(Effect.ALLOW)
                .resources(List.of("*"))
                .build());        
        lambdaRole.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole"));
        
        
        final Function function = Function.Builder.create(this, "IoTexLambdaFunction")
                .role(lambdaRole)
                .runtime(software.amazon.awscdk.services.lambda.Runtime.PYTHON_3_8)    // execution environment
                .code(Code.fromAsset("lambda"))  // code loaded from the "lambda" directory
                .handler("iotex-stream-processing-function.lambda_handler")       
                .build();        

        function.addEventSource(KinesisEventSource.Builder.create(rawStream)
                .startingPosition(StartingPosition.LATEST)
                .build());
                
        
        final String  sql_statement = "CREATE EXTERNAL SCHEMA IF NOT EXISTS kds FROM KINESIS IAM_ROLE default;\n" +  
                    "CREATE MATERIALIZED VIEW iotex_block_view AUTO REFRESH YES AS " +
                    "SELECT approximate_arrival_timestamp,"+
                    "refresh_time,"+
                    "partition_key,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'type\')::text as type,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'version\')::int as version,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'height\')::bigint as height,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'hash\')::text as hash,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'timestamp\')::text as time_stamp,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'prev_block_hash\')::text as prev_block_hash,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'tx_root\')::text as tx_root,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'receipt_root\')::text as receipt_root,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'delta_state_digest\')::text as delta_state_digest,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'producer\')::text as producer,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'signature\')::text as signature,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'number_of_actions\')::bigint as number_of_actions "+
                    "FROM kds.\"processed_iotex_stream\" WHERE json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'type\') in (\'block\');";  
       
       
        Secret secret = redshfit.getSecret();
        CfnWorkgroup workgroup = redshfit.getWorkgroup();
        
        final PolicyStatement statement1 = PolicyStatement.Builder.create()
                        .actions(List.of("redshift:GetClusterCredentials","redshift-serverless:GetCredentials","redshift-data:ExecuteStatement"))
                        .effect(Effect.ALLOW)
                        .resources(List.of("*"))
                        .build();
        final PolicyStatement statement2 = PolicyStatement.Builder.create()
                        .actions(List.of("secretsmanager:GetSecretValue"))
                        .effect(Effect.ALLOW)
                        .resources(List.of(secret.getSecretArn()))
                        .build();   

       final AwsCustomResource awsCustom = AwsCustomResource.Builder.create(this, "aws-custom-execute-sql-iotex")
                .onCreate(AwsSdkCall.builder()
                        .service("RedshiftData")
                        .action("executeStatement")
                        .parameters(Map.of(
                            "Database","dev",
                            "SecretArn", secret.getSecretArn(),
                            "Sql", sql_statement, 
                            "WithEvent", true, 
                            "WorkgroupName", workgroup.getWorkgroupName()))
                        .physicalResourceId(PhysicalResourceId.of("aws-custom-execute-sql-iotex"))
                        .build())
                .policy(AwsCustomResourcePolicy.fromStatements(List.of(statement1, statement2)))
                .build();      
        
    }
    
}