package com.example.demoazure;

import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobDownloadContentResponse;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.DigestUtils;
import org.springframework.util.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

@SpringBootTest
class DemoAzureApplicationTests {

  private final Base64.Encoder encoder = Base64.getEncoder();

  @Value("azure-blob://test-container/data/uploadAndDownloadWithResource.txt")
  Resource sampleText;

  @Test
  void uploadAndDownloadWithResource() throws IOException {
    // Upload
    String content = "uploadAndDownloadWithResource";
    try (OutputStream out = ((WritableResource) sampleText).getOutputStream()) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    }

    // Download
    try (InputStream in = sampleText.getInputStream()) {
      Assertions.assertThat(StreamUtils.copyToString(in, StandardCharsets.UTF_8)).isEqualTo(content);
    }
  }

  @Autowired
  BlobServiceClientBuilder blobServiceClientBuilder;

  @Test
  void uploadAndDownloadWithClientOnDIContainer() {
    // Build client
    BlobClient blobClient = blobServiceClientBuilder.buildClient()
        .getBlobContainerClient("test-container")
        .getBlobClient("data/uploadAndDownloadWithClientOnDIContainer.txt");

    // Upload
    String content = "uploadAndDownloadWithClientOnDIContainer";
    {
      BinaryData uploadData = BinaryData.fromString(content);
      blobClient.uploadWithResponse(
          new BlobParallelUploadOptions(uploadData).setComputeMd5(true), Duration.ofSeconds(10), Context.NONE);
    }

    // Download
    {
      BlobDownloadContentResponse downloadResponse = blobClient.downloadContentWithResponse(
          null, null, Duration.ofSeconds(10), Context.NONE);

      String contentMd5 = encoder.encodeToString(DigestUtils.md5Digest(downloadResponse.getValue().toBytes()));
      String headerMd5 = encoder.encodeToString(downloadResponse.getDeserializedHeaders().getContentMd5());
      Assertions.assertThat(contentMd5).isEqualTo(headerMd5);
      Assertions.assertThat(downloadResponse.getValue().toString()).isEqualTo(content);
    }
  }

  @Test
  void uploadAndDownloadWithClient() {
    String accountName = "kazuki43zoostorage";
    String accountKey = System.getenv("AZURE_STORAGE_ACCOUNT_KEY");
    // Build client
    BlobClient blobClient = new BlobServiceClientBuilder()
        .endpoint("https://" + accountName + ".blob.core.windows.net")
        .credential(new StorageSharedKeyCredential(accountName, accountKey))
        .buildClient()
        .getBlobContainerClient("test-container")
        .getBlobClient("data/uploadAndDownloadWithClient.txt");

    // Upload
    String content = "uploadAndDownloadWithClient";
    {
      BinaryData uploadData = BinaryData.fromString(content);
      blobClient.uploadWithResponse(
          new BlobParallelUploadOptions(uploadData).setComputeMd5(true), Duration.ofSeconds(10), Context.NONE);
    }

    // Download
    {
      BlobDownloadContentResponse downloadResponse = blobClient.downloadContentWithResponse(
          null, null, Duration.ofSeconds(10), Context.NONE);

      String contentMd5 = encoder.encodeToString(DigestUtils.md5Digest(downloadResponse.getValue().toBytes()));
      String headerMd5 = encoder.encodeToString(downloadResponse.getDeserializedHeaders().getContentMd5());
      Assertions.assertThat(contentMd5).isEqualTo(headerMd5);
      Assertions.assertThat(downloadResponse.getValue().toString()).isEqualTo(content);
    }
  }

}
