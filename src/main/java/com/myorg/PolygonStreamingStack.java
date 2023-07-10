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

public class PolygonStreamingStack extends Stack {
    final String REGION  = System.getenv("CDK_DEFAULT_REGION");
    
    public PolygonStreamingStack(final Construct scope, final String id) {
        this(scope, id, null, null, null);
    }    
    
    public PolygonStreamingStack(final Construct scope, final String id, final StackProps props, Vpc vpc, RedshiftServerlessStack redshfit) {
        super(scope, id, props);  
        
        final CfnParameter imageID = CfnParameter.Builder.create(this, "PolygonImageID")
                .type("String")
                .description("The ID of the Polygon full node image")
                .build();
        
        final SecurityGroup sg = SecurityGroup.Builder.create(this, "Polygon-SG")
                .vpc(vpc)
                .allowAllOutbound(true)
                .securityGroupName("Polygon-SG")
                .build();
        sg.addIngressRule(Peer.anyIpv4(), Port.tcp(22), "allow ssh access from anywhere");
        sg.addIngressRule(Peer.anyIpv4(), Port.tcp(30303), "allow Bor service connect our node to other nodes Bor service");  
        sg.addIngressRule(Peer.anyIpv4(), Port.udp(30303), "allow Bor service connect our node to other nodes Bor service");  
        sg.addIngressRule(Peer.anyIpv4(), Port.tcp(26656), "allow Heimdall service connect our node to other nodes Heimdall service"); 
        sg.addIngressRule(Peer.anyIpv4(), Port.udp(26656), "allow Heimdall service connect our node to other nodes Heimdall service"); 
        sg.addIngressRule(Peer.ipv4("127.0.0.1/32"), Port.allTraffic(), "local check"); 
        
        final CfnKeyPair cfnKeyPair = CfnKeyPair.Builder.create(this, "PolygonKeyPair")
                .keyName("PolygonKeyPair")
                .build();
        
        final Role instanceRole = Role.Builder.create(this, "PolygonInstanceRole")
                .assumedBy(new ServicePrincipal("ec2.amazonaws.com"))
                .roleName("PolygonInstanceRole")
                .build(); 
        instanceRole.addToPolicy(PolicyStatement.Builder.create()
                .actions(List.of("kinesis:PutRecord","kinesis:PutRecords"))
                .effect(Effect.ALLOW)
                .resources(List.of("*"))
                .build());    
                
        Instance.Builder.create(this, "Polygon-Full-Node")
                .instanceType(InstanceType.of(InstanceClass.M5D, InstanceSize.XLARGE4))
                .machineImage(new GenericLinuxImage(Map.of(REGION, imageID.getValueAsString())))
                .vpc(vpc)
                .keyName(cfnKeyPair.getKeyName())
                .role(instanceRole)
                .securityGroup(sg)
                .vpcSubnets(SubnetSelection.builder().subnetType(SubnetType.PUBLIC).build())
                .build();

        final Stream rawStream = Stream.Builder.create(this, "raw_polygon_stream")
                .streamName("raw_polygon_stream")
                .build();
                
        final Stream processedSteram = Stream.Builder.create(this, "processed_polygon_stream")
                .streamName("processed_polygon_stream")
                .build();     
        
  
        final Role lambdaRole = Role.Builder.create(this, "PolygonLambdaStreamingRole")
                .assumedBy(new ServicePrincipal("lambda.amazonaws.com"))
                .roleName("PolygonLambdaStreamingRole")
                .build();
        lambdaRole.addToPolicy(PolicyStatement.Builder.create()
                .actions(List.of("kinesis:PutRecords"))
                .effect(Effect.ALLOW)
                .resources(List.of("*"))
                .build());        
        lambdaRole.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole"));
        
        
        final Function function = Function.Builder.create(this, "PolygonLambdaFunction")
                .role(lambdaRole)
                .runtime(software.amazon.awscdk.services.lambda.Runtime.PYTHON_3_8)    // execution environment
                .code(Code.fromAsset("lambda"))  // code loaded from the "lambda" directory
                .handler("polygon-stream-processing-function.lambda_handler")        
                .build();        

        function.addEventSource(KinesisEventSource.Builder.create(rawStream)
                .startingPosition(StartingPosition.LATEST)
                .build());
                
        
        final String  sql_statement = "CREATE EXTERNAL SCHEMA IF NOT EXISTS kds FROM KINESIS IAM_ROLE default;\n" +  
                    "CREATE MATERIALIZED VIEW polygon_block_view AUTO REFRESH YES AS " +
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
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'difficulty\')::bigint as difficulty,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'total_difficulty\')::bigint as total_difficulty,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'size\')::bigint as size,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'extra_data\')::text as extra_data,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'gas_limit\')::bigint as gas_limit,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'gas_used\')::bigint as gas_used,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'timestamp\')::int as timestamp,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'transaction_count\')::bigint as transaction_count,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'base_fee_per_gas\')::text as base_fee_per_gas,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'item_id\')::text as item_id,"+
                    "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'item_timestamp\')::text as item_timestamp "+
                    "FROM kds.\"processed_polygon_stream\" WHERE json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'type\') in (\'block\');";  
       
       
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

       final AwsCustomResource awsCustom = AwsCustomResource.Builder.create(this, "aws-custom-execute-sql-polygon")
                .onCreate(AwsSdkCall.builder()
                        .service("RedshiftData")
                        .action("executeStatement")
                        .parameters(Map.of(
                            "Database","dev",
                            "SecretArn", secret.getSecretArn(),
                            "Sql", sql_statement, 
                            "WithEvent", true, 
                            "WorkgroupName", workgroup.getWorkgroupName()))
                        .physicalResourceId(PhysicalResourceId.of("aws-custom-execute-sql-polygon"))
                        .build())
                .policy(AwsCustomResourcePolicy.fromStatements(List.of(statement1, statement2)))
                .build();      
        
    }
    
}