package com.myorg;

import software.constructs.Construct;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.redshiftserverless.*;
import software.amazon.awscdk.services.ec2.*;
import software.amazon.awscdk.services.iam.*;
import software.amazon.awscdk.customresources.*;
import java.util.*;
import software.amazon.awscdk.services.secretsmanager.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

public class RedshiftServerlessStack extends Stack {
    private final String REDSHIFT_NAMESPACE_NAME = "blockchain-namespace";
    private final String REDSHIFT_WORKGROUP_NAME = "blockchain-workgroup";
    private final String ADMIN_USER_NAME = "web3workshop";
    private final String DB_NAME = "dev";
    private Secret secret = null;
    private Role redshiftStreamingRole = null;
    private CfnNamespace namespace = null;
    private CfnWorkgroup workgroup = null;
    private SecurityGroup sg2 = null;
    
    public RedshiftServerlessStack(final Construct scope, final String id) {
        this(scope, id, null, null);
    }    
    
    public RedshiftServerlessStack(final Construct scope, final String id, final StackProps props, Vpc vpc) {
        super(scope, id, props);  
        
        final SecurityGroup sg1 = SecurityGroup.Builder.create(this, "Quicksight-SG")
                .vpc(vpc)
                .allowAllOutbound(true)
                .securityGroupName("Quicksight-SG")
                .build();
        sg2 = SecurityGroup.Builder.create(this, "Redshift-SG")
                .vpc(vpc)
                .allowAllOutbound(true)
                .securityGroupName("Redshift-SG")
                .build();
        sg2.addIngressRule(Peer.securityGroupId(sg1.getSecurityGroupId()), Port.tcp(5439), "allow quicksight access on port 5439");
        
        
        //Secret secret = null;
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
        
        
        redshiftStreamingRole = Role.Builder.create(this, "RedshiftStreamingRole")
                .assumedBy(new CompositePrincipal(new ServicePrincipal("redshift.amazonaws.com"), new ServicePrincipal("redshift-serverless.amazonaws.com")))
                .roleName("RedshiftStreamingRole")
                .build();
        redshiftStreamingRole.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("AmazonKinesisReadOnlyAccess"));
        redshiftStreamingRole.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("AmazonRedshiftQueryEditorV2FullAccess"));
         
        namespace = CfnNamespace.Builder.create(this, "RedshiftServerlessNamespace")
                .namespaceName(REDSHIFT_NAMESPACE_NAME)
                .adminUsername(ADMIN_USER_NAME)
                .adminUserPassword(secret.secretValueFromJson("password").unsafeUnwrap())
                .dbName(DB_NAME)
                .defaultIamRoleArn(redshiftStreamingRole.getRoleArn())
                .iamRoles(List.of(redshiftStreamingRole.getRoleArn()))
                .build();  

        workgroup = CfnWorkgroup.Builder.create(this, "RedshiftServerlessWorkgroup")
                .workgroupName(REDSHIFT_WORKGROUP_NAME)
                .baseCapacity(8)
                .namespaceName(namespace.getNamespaceName())
                .securityGroupIds(List.of(sg2.getSecurityGroupId()))
                .subnetIds(vpc.selectSubnets().getSubnetIds())
                .build();  
        workgroup.getNode().addDependency(namespace);        

    }
    
    public CfnNamespace getNamespace() {
        return namespace;
    }
    
    public CfnWorkgroup getWorkgroup() {
        return workgroup;
    }
    
    public Secret getSecret() {
        return secret;
    }
    
    public Role getIamRole() {
        return redshiftStreamingRole;
    }
    
    public SecurityGroup getSecurityGroup() {
        return sg2;
    }
}