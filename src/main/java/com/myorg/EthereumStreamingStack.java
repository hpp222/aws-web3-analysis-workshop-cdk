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

public class EthereumStreamingStack extends Stack {
    final String REGION  = System.getenv("CDK_DEFAULT_REGION");
    
    public EthereumStreamingStack(final Construct scope, final String id) {
        this(scope, id, null, null, null);
    }    
    
    public EthereumStreamingStack(final Construct scope, final String id, final StackProps props, Vpc vpc, RedshiftServerlessStack redshfit) {
        super(scope, id, props);  
        
        final CfnParameter imageID = CfnParameter.Builder.create(this, "EthereumImageID")
                .type("String")
                .description("The ID of the Ethereum full node image")
                .build();
        
        // create a security group for Ethereum instance
        final SecurityGroup sg = SecurityGroup.Builder.create(this, "Ethereum-SG")
                .vpc(vpc)
                .allowAllOutbound(true)
                .securityGroupName("Ethereum-SG")
                .build();
        sg.addIngressRule(Peer.anyIpv4(), Port.tcp(22), "allow ssh access from anywhere");
        sg.addIngressRule(Peer.anyIpv4(), Port.tcp(30303), "Geth runs on port 30303 for both TCP and UDP");  
        sg.addIngressRule(Peer.anyIpv4(), Port.udp(30303), "Geth runs on port 30303 for both TCP and UDP");  
        sg.addIngressRule(Peer.anyIpv4(), Port.tcp(9000), "Lighthouse, by default, uses port 9000 for both TCP and UDP"); 
        sg.addIngressRule(Peer.anyIpv4(), Port.udp(9000), "Lighthouse, by default, uses port 9000 for both TCP and UDP"); 
        sg.addIngressRule(Peer.ipv4("127.0.0.1/32"), Port.allTraffic(), "local check"); 
        
        // crete a key pair
        final CfnKeyPair cfnKeyPair = CfnKeyPair.Builder.create(this, "EthereumKeyPair")
                .keyName("EthereumKeyPair")
                .build();
        
        final Role instanceRole = Role.Builder.create(this, "EthereumInstanceRole")
                .assumedBy(new ServicePrincipal("ec2.amazonaws.com"))
                .roleName("EthereumInstanceRole")
                .build(); 
        instanceRole.addToPolicy(PolicyStatement.Builder.create()
                .actions(List.of("kinesis:PutRecord","kinesis:PutRecords"))
                .effect(Effect.ALLOW)
                .resources(List.of("*"))
                .build());    
                
        // create instance from Ethereum full node image
        Instance.Builder.create(this, "Ethereum-Full-Node")
                .instanceType(InstanceType.of(InstanceClass.M6G, InstanceSize.XLARGE2))
                .machineImage(new GenericLinuxImage(Map.of(REGION, imageID.getValueAsString())))
                .vpc(vpc)
                .keyName(cfnKeyPair.getKeyName())
                .role(instanceRole)
                .securityGroup(sg)
                .vpcSubnets(SubnetSelection.builder().subnetType(SubnetType.PUBLIC).build())
                .build();

        final Stream rawStream = Stream.Builder.create(this, "raw_ethereum_stream")
                .streamName("raw_ethereum_stream")
                .build();
                
        final Stream processedSteram = Stream.Builder.create(this, "processed_ethereum_stream")
                .streamName("processed_ethereum_stream")
                .build();     
        
        // assign kds write permissions and log premissions to lambda function    
        final Role lambdaRole = Role.Builder.create(this, "EthLambdaStreamingRole")
                .assumedBy(new ServicePrincipal("lambda.amazonaws.com"))
                .roleName("EthLambdaStreamingRole")
                .build();
        lambdaRole.addToPolicy(PolicyStatement.Builder.create()
                .actions(List.of("kinesis:PutRecords"))
                .effect(Effect.ALLOW)
                .resources(List.of("*"))
                .build());        
        lambdaRole.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole"));
        
        
        // Defines a lambda resource
        final Function function = Function.Builder.create(this, "EthLambdaFunction")
                .role(lambdaRole)
                .runtime(software.amazon.awscdk.services.lambda.Runtime.PYTHON_3_8)    // execution environment
                .code(Code.fromAsset("lambda"))  // code loaded from the "lambda" directory
                .handler("ethereum-stream-processing-function.lambda_handler")        // file is "stream-data-processing-function", function is "lambda_handler"
                .build();        

        function.addEventSource(KinesisEventSource.Builder.create(rawStream)
                .startingPosition(StartingPosition.LATEST)
                .build());
                
        
        final String  sql_statement = "CREATE EXTERNAL SCHEMA IF NOT EXISTS kds FROM KINESIS IAM_ROLE default;\n" +  
                    "CREATE MATERIALIZED VIEW eth_block_view AUTO REFRESH YES AS " +
                    "SELECT approximate_arrival_timestamp,"+
                    "refresh_time,"+
                    "partition_key,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'type\')::text as type,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'number\')::bigint as number,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'hash\')::text as hash,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'parent_hash\')::text as parent_hash,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'nonce\')::text as nonce,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'sha3_uncles\')::text as sha3_uncles,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'logs_bloom\')::text as logs_bloom,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'transactions_root\')::text as transactions_root,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'state_root\')::text as state_root,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'receipts_root\')::text as receipts_root,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'miner\')::text as miner,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'difficulty\')::numeric(38) as difficulty,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'total_difficulty\')::text as total_difficulty,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'size\')::bigint as size,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'extra_data\')::text as extra_data,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'gas_limit\')::bigint as gas_limit,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'gas_used\')::bigint as gas_used,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'timestamp\')::int as timestamp,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'transaction_count\')::bigint as transaction_count,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'item_id\')::text as item_id,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'item_timestamp\')::text as item_timestamp "+
                    "FROM kds.\"processed_ethereum_stream\" WHERE json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'type\') in (\'block\');";  
       
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

       final AwsCustomResource awsCustom = AwsCustomResource.Builder.create(this, "aws-custom-execute-sql-eth")
                .onCreate(AwsSdkCall.builder()
                        .service("RedshiftData")
                        .action("executeStatement")
                        .parameters(Map.of(
                            "Database","dev",
                            "SecretArn", secret.getSecretArn(),
                            "Sql", sql_statement, 
                            "WithEvent", true, 
                            "WorkgroupName", workgroup.getWorkgroupName()))
                        .physicalResourceId(PhysicalResourceId.of("aws-custom-execute-sql-eth"))
                        .build())
                .policy(AwsCustomResourcePolicy.fromStatements(List.of(statement1, statement2)))
                .build();      
        //awsCustom.getNode().addDependency(workgroup); 
        
    }
    
}