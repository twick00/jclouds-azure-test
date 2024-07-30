package org.example;

import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.jclouds.ContextBuilder;
import org.jclouds.azure.storage.AzureStorageResponseException;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.io.Payload;
import org.jclouds.io.Payloads;

public class Main {

  static String storageAccountName = "";
  static String storageAccountKey = "";
  static String containerName = "";
  static String endpoint = "";
  static String blobFullyQualifiedFileName = "./test_files/hello_world1.txt";
  static String blobName = ".txt";
  static int numParts = 300;

  public static void main(String[] args) throws IOException {
    BlobStoreContext context = ContextBuilder.newBuilder("azureblob")
        .endpoint(endpoint)
        .credentials(storageAccountName, storageAccountKey)
        .buildView(BlobStoreContext.class);

    // Access the BlobStore
    BlobStore blobStore = context.getBlobStore();

    // Create a Container
    blobStore.createContainerInLocation(null, containerName);

    // Create a blob. 
    ByteSource byteSource = Files.asByteSource(new File(blobFullyQualifiedFileName));

    byte[] fileBytes = byteSource.read();
    int partSize = (int) Math.ceil((double) fileBytes.length / numParts);

    // Split the file into 18 parts
    List<String> parts = new ArrayList<>();
    for (int i = 0; i < fileBytes.length; i += partSize) {
      int end = Math.min(fileBytes.length, i + partSize);
      byte[] part = new byte[end - i];
      System.arraycopy(fileBytes, 0, part, 0, part.length);
      parts.add(new String(part, StandardCharsets.UTF_8));
    }

    blobName = new Random().nextInt(1000) + blobName;

    MultipartUpload multipartUpload = blobStore.initiateMultipartUpload(
        containerName,
        blobStore.blobBuilder(blobName).build().getMetadata(),
        new PutOptions()
    );

    // Upload the first failing part
    int firstFailingPart = 248; // Correlates to 'AAAA-A=='
    MultipartUpload mpu = MultipartUpload.create(
        containerName,
        blobName,
        multipartUpload.id(),
        multipartUpload.blobMetadata(),
        multipartUpload.putOptions()
    );
    Payload payload = Payloads.newStringPayload(parts.get(firstFailingPart));
    try {
      blobStore.uploadMultipartPart(mpu, firstFailingPart, payload);
    } catch (AzureStorageResponseException e) {
      System.out.println("Error uploading part " + firstFailingPart);
      blobStore.abortMultipartUpload(mpu);
      e.printStackTrace();
      System.exit(1);
    }

    /**
     * There are two ways to test this; one is to use the commented code below, and the other is to use the code above.
     * The code above shows that specific part numbers will always fail, while the code below simply uploads all parts and fails near the end
     */

//    // For each part, upload it to the blobstore
//    for (int i = 0; i < parts.size(); i++) {
//      if (i % 10 == 0) {
//        System.out.println("Uploaded " + i + " parts");
//      }
//      MultipartUpload mpu = MultipartUpload.create(
//          containerName,
//          blobName,
//          multipartUpload.id(),
//          multipartUpload.blobMetadata(),
//          multipartUpload.putOptions()
//      );
//
//      Payload payload = Payloads.newStringPayload(parts.get(i));
//      try {
//        blobStore.uploadMultipartPart(mpu, i, payload);
//      } catch (AzureStorageResponseException e) {
//        System.out.println("Error uploading part " + i);
//        blobStore.abortMultipartUpload(mpu);
//        e.printStackTrace();
//        System.exit(1);
//      }
//    }

    List<MultipartPart> multipartParts = blobStore.listMultipartUpload(multipartUpload);

    // Complete the multipart upload
    String s = blobStore.completeMultipartUpload(multipartUpload, multipartParts);
    System.out.println(s);
    context.close();
  }
}
