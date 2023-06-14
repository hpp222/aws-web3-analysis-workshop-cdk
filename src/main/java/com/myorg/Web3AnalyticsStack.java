package com.myorg;

import java.util.*;
import software.constructs.Construct;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.ec2.*;

import software.amazon.awscdk.services.iam.*;
import software.amazon.awscdk.CfnParameter;

import software.amazon.awscdk.services.kinesis.*;
import software.amazon.awscdk.services.lambda.*;
import software.amazon.awscdk.services.lambda.eventsources.*;

import software.amazon.awscdk.services.redshiftserverless.*;
import software.amazon.awscdk.customresources.*;
import software.amazon.awscdk.services.secretsmanager.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;


public class Web3AnalyticsStack extends Stack {
    private final Vpc vpc;
    
    public Web3AnalyticsStack(final Construct scope, final String id) {
        this(scope, id, null);
    }    
    public Web3AnalyticsStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);  
        
        CfnParameter imageID = CfnParameter.Builder.create(this, "ImageID")
                .type("String")
                .description("The ID of the Ethereum full node image")
                .build();
        
        /* create a VPC */
        final String KEY_NAME = "Web3KeyPair";
        final String REGION   = System.getenv("CDK_DEFAULT_REGION");
        
        vpc = Vpc.Builder.create(this, "Web3VPC")
                .subnetConfiguration(List.of(SubnetConfiguration.builder()
                    .name("Public")
                    .cidrMask(24)
                    .subnetType(SubnetType.PUBLIC)
                    .build()))
                .vpcName("Web3VPC")    
                .build();  
        
        /* 
         * create an EC2 instance for Ethereum full node deployment 
         * 1. create a security group for Ethereum instance
         * 2. create a keypair for accessing the isntance
         * 3. create an instance role
         * 4. create an instance from suitable image
        */
        final SecurityGroup instanceSG = SecurityGroup.Builder.create(this, "Ethereum-SG")
                .vpc(vpc)
                .allowAllOutbound(true)
                .securityGroupName("Ethereum-SG")
                .build();
        instanceSG.addIngressRule(Peer.anyIpv4(), Port.tcp(22), "allow ssh access from anywhere");
        instanceSG.addIngressRule(Peer.anyIpv4(), Port.tcp(30303), "Geth runs on port 30303 for external listening");  
        instanceSG.addIngressRule(Peer.anyIpv4(), Port.tcp(9000), "Lighthouse, by default, uses port 9000 for both TCP and UDP"); 
        
        final CfnKeyPair cfnKeyPair = CfnKeyPair.Builder.create(this, KEY_NAME)
                .keyName(KEY_NAME)
                .build();
                
        final Role instanceRole = Role.Builder.create(this, "Web3WorkshopInstanceRole")
                .assumedBy(new ServicePrincipal("ec2.amazonaws.com"))
                .roleName("Web3WorkshopInstanceRole")
                .build(); 
        instanceRole.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("AmazonSSMManagedInstanceCore"));   

        final Instance instance = Instance.Builder.create(this, "Eth-Instance")
                .instanceType(InstanceType.of(InstanceClass.M5, InstanceSize.XLARGE2))
                .machineImage(new GenericLinuxImage(Map.of(REGION, imageID.getValueAsString())))
                .vpc(vpc)
                .keyName(KEY_NAME)
                .role(instanceRole)
                .securityGroup(instanceSG)
                .vpcSubnets(SubnetSelection.builder().subnetType(SubnetType.PUBLIC).build())
                .build();
        instance.getNode().addDependency(vpc);    
        
        
        /* create Kinesis data streams*/
        final Stream rawStream = Stream.Builder.create(this, "raw_blockchain_kds")
                .streamName("raw_blockchain_kds")
                .build();
        final Stream processedSteram = Stream.Builder.create(this, "processed_blockchain_kds")
                .streamName("processed_blockchain_kds")
                .build();    
                
        /*
         * create a lambda function to process raw stream 
         * and write the processed data to another stream
         * 1. assign required permissions to the function
         * 2. create a lambda funciton
         * 3. add raw stream as the event trigger of lambda
        */   
        final Role lambdaRole = Role.Builder.create(this, "LambdaStreamingRole")
                .assumedBy(new ServicePrincipal("lambda.amazonaws.com"))
                .roleName("LambdaStreamingRole")
                .build();
        lambdaRole.addToPolicy(PolicyStatement.Builder.create()
                .actions(List.of("kinesis:PutRecords"))
                .effect(Effect.ALLOW)
                .resources(List.of("*"))
                .build());        
        lambdaRole.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole"));  
        
        final Function kdsFunction = Function.Builder.create(this, "KDSFunction")
                .role(lambdaRole)
                .runtime(software.amazon.awscdk.services.lambda.Runtime.PYTHON_3_8)    // execution environment
                .code(Code.fromAsset("lambda"))  // code loaded from the "lambda" directory
                .handler("stream-data-processing-function.lambda_handler")        // file is "stream-data-processing-function", function is "lambda_handler"
                .build();  
        
        kdsFunction.addEventSource(KinesisEventSource.Builder.create(rawStream)
                .batchSize(100) // default
                .startingPosition(StartingPosition.LATEST)
                .build());        
        
        /* create redshift serverless resources*/        
        final String REDSHIFT_NAMESPACE_NAME = "blockchain-namespace";
        final String REDSHIFT_WORKGROUP_NAME = "blockchain-workgroup";
        final String ADMIN_USER_NAME = "web3workshop";
        final String DB_NAME = "dev";        
        
        final SecurityGroup quicksightSG = SecurityGroup.Builder.create(this, "Quicksight-SG")
                .vpc(vpc)
                .allowAllOutbound(true)
                .securityGroupName("Quicksight-SG")
                .build();
        final SecurityGroup redshiftSG = SecurityGroup.Builder.create(this, "Redshift-SG")
                .vpc(vpc)
                .allowAllOutbound(true)
                .securityGroupName("Redshift-SG")
                .build();
        redshiftSG.addIngressRule(Peer.securityGroupId(quicksightSG.getSecurityGroupId()), Port.tcp(5439), "allow quicksight access on port 5439");
        
        
        Secret secret = null;
        try{
            secret = Secret.Builder.create(this, "RedshiftSecret")
                    .generateSecretString(SecretStringGenerator.builder()
                            .secretStringTemplate(new ObjectMapper().writeValueAsString(Map.of("username", ADMIN_USER_NAME)))
                            .generateStringKey("password")
                            .excludeCharacters("!\"'()*+,-./:;<=>?@[\\]^_`{|}~")
                            .build())
                    .build();
        } catch(JsonProcessingException e) {
            e.printStackTrace();
        }
        
        final Role redshiftStreamingRole = Role.Builder.create(this, "RedshiftStreamingRole")
                .assumedBy(new CompositePrincipal(new ServicePrincipal("redshift.amazonaws.com"), new ServicePrincipal("redshift-serverless.amazonaws.com")))
                .roleName("RedshiftStreamingRole")
                .build();
        redshiftStreamingRole.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("AmazonKinesisReadOnlyAccess"));
        redshiftStreamingRole.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("AmazonRedshiftQueryEditorV2FullAccess"));     
        
        final CfnNamespace namespace = CfnNamespace.Builder.create(this, "RedshiftServerlessNamespace")
                .namespaceName(REDSHIFT_NAMESPACE_NAME)
                .adminUsername(ADMIN_USER_NAME)
                .adminUserPassword(secret.secretValueFromJson("password").unsafeUnwrap())
                .dbName(DB_NAME)
                .defaultIamRoleArn(redshiftStreamingRole.getRoleArn())
                .iamRoles(List.of(redshiftStreamingRole.getRoleArn()))
                .build();  
        //namespace.getNode().addDependency(secret); 

        final CfnWorkgroup workgroup = CfnWorkgroup.Builder.create(this, "RedshiftServerlessWorkgroup")
                .workgroupName(REDSHIFT_WORKGROUP_NAME)
                .baseCapacity(8)
                .namespaceName(namespace.getNamespaceName())
                .securityGroupIds(List.of(redshiftSG.getSecurityGroupId()))
                .subnetIds(vpc.selectSubnets().getSubnetIds())
                .build();  
        workgroup.getNode().addDependency(namespace);    
        
        final String  sql_statement = "CREATE EXTERNAL SCHEMA kds FROM KINESIS IAM_ROLE default;\n" +  
                        "CREATE MATERIALIZED VIEW block_view AUTO REFRESH YES AS " +
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
                        "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'total_difficulty\')::numeric(38) as total_difficulty,"+
                        "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'size\')::bigint as size,"+
                        "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'extra_data\')::text as extra_data,"+
                        "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'gas_limit\')::bigint as gas_limit,"+
                        "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'gas_used\')::bigint as gas_used,"+
                        "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'timestamp\')::int as timestamp,"+
                        "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'transaction_count\')::bigint as transaction_count,"+
                        "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'item_id\')::text as item_id,"+
                        "json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'item_timestamp\')::text as item_timestamp "+
                        "FROM kds.\"processed_blockchain_kds\" WHERE json_extract_path_text(from_varbyte(kinesis_data, \'utf-8\'),\'type\') in (\'block\');";     
        
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
        final AwsCustomResource awsCustom = AwsCustomResource.Builder.create(this, "aws-custom-execute-sql")
                .onCreate(AwsSdkCall.builder()
                        .service("RedshiftData")
                        .action("executeStatement")
                        .parameters(Map.of(
                            "Database","dev",
                            "SecretArn", secret.getSecretArn(),
                            "Sql", sql_statement, 
                            "WithEvent", true, 
                            "WorkgroupName", REDSHIFT_WORKGROUP_NAME))
                        .physicalResourceId(PhysicalResourceId.of("physicalResourceRedshiftServerlessSQLStatement"))
                        .build())
                .policy(AwsCustomResourcePolicy.fromStatements(List.of(statement1, statement2)))
                .build();      
        awsCustom.getNode().addDependency(workgroup);         
    }
}
