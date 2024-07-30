## Minimum example of Azure Blob Storage failing blockId is `-` or `_`.

To run, update the following variables based on your Azure Storage Account:
```
  static String storageAccountName = "";
  static String storageAccountKey = "";
  static String containerName = "";
  static String endpoint = "";
```

Then run the following command:
```bash
mvn clean package
mvn exec:java -Dexec.mainClass="org.example.Main"
```
