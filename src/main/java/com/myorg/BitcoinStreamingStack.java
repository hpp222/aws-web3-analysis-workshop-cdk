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

public class BitcoinStreamingStack extends Stack {
    final String REGION  = System.getenv("CDK_DEFAULT_REGION");
    
    public BitcoinStreamingStack(final Construct scope, final String id) {
        this(scope, id, null, null, null);
    }    
    
    public BitcoinStreamingStack(final Construct scope, final String id, final StackProps props, Vpc vpc, RedshiftServerlessStack redshfit) {
        super(scope, id, props);  
        
        final CfnParameter imageID = CfnParameter.Builder.create(this, "BitcoinImageID")
                .type("String")
                .description("The ID of the Bitcoin full node image")
                .build();
        
        final SecurityGroup sg = SecurityGroup.Builder.create(this, "Bitcoin-SG")
                .vpc(vpc)
                .allowAllOutbound(true)
                .securityGroupName("Bitcoin-SG")
                .build();
        sg.addIngressRule(Peer.anyIpv4(), Port.tcp(22), "allow ssh access from anywhere");
        sg.addIngressRule(Peer.anyIpv4(), Port.tcp(80), "for health monitoring");  
        sg.addIngressRule(Peer.anyIpv4(), Port.tcp(18332), "for syncing");  
        
        final CfnKeyPair cfnKeyPair = CfnKeyPair.Builder.create(this, "BitcoinKeyPair")
                .keyName("BitcoinKeyPair")
                .build();
        
        final Role instanceRole = Role.Builder.create(this, "BitcoinInstanceRole")
                .assumedBy(new ServicePrincipal("ec2.amazonaws.com"))
                .roleName("BitcoinInstanceRole")
                .build(); 
        instanceRole.addToPolicy(PolicyStatement.Builder.create()
                .actions(List.of("kinesis:PutRecord","kinesis:PutRecords"))
                .effect(Effect.ALLOW)
                .resources(List.of("*"))
                .build());    
                
        Instance.Builder.create(this, "Bitcoin-Full-Node")
                .instanceType(InstanceType.of(InstanceClass.T3, InstanceSize.MEDIUM))
                .machineImage(new GenericLinuxImage(Map.of(REGION, imageID.getValueAsString())))
                .vpc(vpc)
                .keyName(cfnKeyPair.getKeyName())
                .role(instanceRole)
                .securityGroup(sg)
                .vpcSubnets(SubnetSelection.builder().subnetType(SubnetType.PUBLIC).build())
                .build();

        final Stream rawStream = Stream.Builder.create(this, "raw_bitcoin_stream")
                .streamName("raw_bitcoin_stream")
                .build();
                
        final Stream processedSteram = Stream.Builder.create(this, "processed_bitcoin_stream")
                .streamName("processed_bitcoin_stream")
                .build();     
        
        final Role lambdaRole = Role.Builder.create(this, "BitcoinLambdaStreamingRole")
                .assumedBy(new ServicePrincipal("lambda.amazonaws.com"))
                .roleName("BitcoinLambdaStreamingRole")
                .build();
        lambdaRole.addToPolicy(PolicyStatement.Builder.create()
                .actions(List.of("kinesis:PutRecords","kinesis:PutRecord"))
                .effect(Effect.ALLOW)
                .resources(List.of("*"))
                .build());        
        lambdaRole.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole"));
        
        
        final Function function = Function.Builder.create(this, "BitcoinLambdaFunction")
                .role(lambdaRole)
                .runtime(software.amazon.awscdk.services.lambda.Runtime.PYTHON_3_8)    // execution environment
                .code(Code.fromAsset("lambda"))  // code loaded from the "lambda" directory
                .handler("bitcoin-stream-processing-function.lambda_handler")       
                .build();        

        function.addEventSource(KinesisEventSource.Builder.create(rawStream)
                .startingPosition(StartingPosition.LATEST)
                .build());
                
        
        final String  sql_statement = "CREATE EXTERNAL SCHEMA IF NOT EXISTS kds FROM KINESIS IAM_ROLE default;\n" +  
                    "CREATE MATERIALIZED VIEW bitcoin_block_view AUTO REFRESH YES AS " +
                    "SELECT approximate_arrival_timestamp,"+
                    "refresh_time,"+
                    "partition_key,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'type\')::text as type,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'hash\')::text as hash,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'size\')::bigint as size,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'weight\')::bigint as weight,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'number\')::bigint as number,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'version\')::bigint as version,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'merkle_root\')::text as merkle_root,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'timestamp\')::text as time_stamp,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'nonce\')::text as nonce,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'bits\')::text as bits,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'coinbase_param\')::text as coinbase_param,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'transaction_count\')::bigint as transaction_count "+
                    "FROM kds.\"processed_bitcoin_stream\" WHERE json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'type\') in (\'block\');";  
       
       
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

       final AwsCustomResource awsCustom = AwsCustomResource.Builder.create(this, "aws-custom-execute-sql-bitcoin")
                .onCreate(AwsSdkCall.builder()
                        .service("RedshiftData")
                        .action("executeStatement")
                        .parameters(Map.of(
                            "Database","dev",
                            "SecretArn", secret.getSecretArn(),
                            "Sql", sql_statement, 
                            "WithEvent", true, 
                            "WorkgroupName", workgroup.getWorkgroupName()))
                        .physicalResourceId(PhysicalResourceId.of("aws-custom-execute-sql-bitcoin"))
                        .build())
                .policy(AwsCustomResourcePolicy.fromStatements(List.of(statement1, statement2)))
                .build();      
        
    }
    
}
