package pingali.jeevan.aws.dynamodb.local;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.ArrayList;
import java.util.List;

public class Test1 {

    public static void main(String[] args) throws Exception {
        System.setProperty("sqlite4java.library.path", "native-libs");
        String port = "8000";
        DynamoDBProxyServer server = ServerRunner.createServerFromCommandLineArgs(
                new String[]{"-inMemory", "-port", port});
        server.start();

        BasicAWSCredentials awsCreds = new BasicAWSCredentials("access_key_id", "secret_key_id");

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(
                                "http://localhost:8000", "us-east-1"))
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .build();
        DynamoDB dynamoDB = new DynamoDB(client);

        String tableName = "test1_table";

        List<AttributeDefinition> attributeDefinitions= new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("Id").withAttributeType("N"));

        List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
        keySchema.add(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.HASH));

        CreateTableRequest request = new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(5L)
                        .withWriteCapacityUnits(6L));

        Table table = dynamoDB.createTable(request);

        table.waitForActive();

        TableDescription tableDescription =
                dynamoDB.getTable(tableName).describe();

        System.out.printf("%s: %s \t ReadCapacityUnits: %d \t WriteCapacityUnits: %d",
                tableDescription.getTableStatus(),
                tableDescription.getTableName(),
                tableDescription.getProvisionedThroughput().getReadCapacityUnits(),
                tableDescription.getProvisionedThroughput().getWriteCapacityUnits());
        table.delete();

        System.out.println("Stopping dynamoDB");
        dynamoDB.shutdown();
        System.out.println("Stopping client");
        client.shutdown();

        System.out.println("Stopping seever");
        server.stop();
        System.out.println("Done");
    }
}
