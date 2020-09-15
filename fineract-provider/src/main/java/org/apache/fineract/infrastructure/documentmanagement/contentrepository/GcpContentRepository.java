/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fineract.infrastructure.documentmanagement.contentrepository;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageOptions;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;

import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import org.apache.fineract.infrastructure.core.domain.Base64EncodedImage;
import org.apache.fineract.infrastructure.documentmanagement.command.DocumentCommand;
import org.apache.fineract.infrastructure.documentmanagement.data.DocumentData;
import org.apache.fineract.infrastructure.documentmanagement.data.FileData;
import org.apache.fineract.infrastructure.documentmanagement.data.ImageData;
import org.apache.fineract.infrastructure.documentmanagement.domain.StorageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcpContentRepository implements ContentRepository {

    private static final Logger LOG = LoggerFactory.getLogger(GcpContentRepository.class);
    private final Bucket gcpBucket;
    private final Storage storage;

    public GcpContentRepository(final String gcpBucketName, final String gcpBucketLocation, final String gcpProjectId) {
        // set credentials here
        this.storage = StorageOptions.newBuilder().setProjectId(gcpProjectId).build().getService();
        final StorageClass storageClass = StorageClass.COLDLINE;
        this.gcpBucket = storage.create(BucketInfo.newBuilder(gcpBucketName).setStorageClass(storageClass)
                .setLocation(gcpBucketLocation).build());
        LOG.info("Created storage bucket: " + this.gcpBucket.getName() + " of class: "
                + this.gcpBucket.getStorageClass().toString() + " at: " + this.gcpBucket.getLocation());
    }

    @Override
    public String saveFile(final InputStream uploadedInputStream, final DocumentCommand documentCommand) {
        String fileName = documentCommand.getFileName();
        ContentRepositoryUtils.validateFileSizeWithinPermissibleRange(documentCommand.getSize(), fileName);
        final String uploadDocFolder = "documents" + File.separator + documentCommand.getParentEntityType().toString()
                + File.separator + documentCommand.getParentEntityId() + File.separator
                + ContentRepositoryUtils.generateRandomString();
        String uploadDocFullPath = uploadDocFolder + File.separator + fileName;
        uploadDocument(fileName, uploadedInputStream);
        return uploadDocFullPath;
    }

    @Override
    public void deleteFile(String fileName, String documentPath){}

    @Override
    public FileData fetchFile(DocumentData documentData){
        return null;
    }

    @Override
    public String saveImage(InputStream uploadedInputStream, Long resourceId, String imageName, Long fileSize){
        return null;
    }

    @Override
    public String saveImage(Base64EncodedImage base64EncodedImage, Long resourceId, String imageName){
        return null;
    }

    @Override
    public void deleteImage(Long resourceId, String location){}

    @Override
    public ImageData fetchImage(ImageData imageData){
        return null;
    }

    @Override
    public StorageType getStorageType(){
        return null;
    }

    private void uploadDocument(String objectName, InputStream content) throws IOException {
        try {
            final BlobId blobId = BlobId.of(this.gcpBucket.getName(), objectName);
            final Hasher hasher = Hashing.crc32c().newHasher().putBytes(content.readAllBytes());
            final String crc32c = BaseEncoding.base64().encode(Ints.toByteArray(hasher.hash().asInt()));
            final BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setCrc32c(crc32c).build();
            storage.createFrom(blobInfo, content, Storage.BlobWriteOption.crc32cMatch());
            LOG.info( objectName + " uploaded to bucket " + gcpBucket.getName());
        } catch (IOException e){
            final String message = e.getMessage();
            LOG.error(message);
        }
    }
}
