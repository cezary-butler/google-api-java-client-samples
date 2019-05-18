/*
 * Copyright (c) 2012 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.api.services.samples.drive.cmdline;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.media.MediaHttpDownloader;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Preconditions;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.About;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.common.util.concurrent.AtomicDouble;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.api.client.googleapis.media.MediaHttpDownloader.DownloadState.MEDIA_COMPLETE;

/**
 * A sample application that runs multiple requests against the Drive API. The requests this sample
 * makes are:
 * <ul>
 * <li>Does a resumable media upload</li>
 * <li>Updates the uploaded file by renaming it</li>
 * <li>Does a resumable media download</li>
 * <li>Does a direct media upload</li>
 * <li>Does a direct media download</li>
 * </ul>
 *
 * @author rmistry@google.com (Ravi Mistry)
 */
public class DriveSample {

  private static final int THREAD_COUNT =6;
  /**
   * Be sure to specify the name of your application. If the application name is {@code null} or
   * blank, the application will log a warning. Suggested format is "MyCompany-ProductName/1.0".
   */
  private static final String APPLICATION_NAME = "DriveDownloads2";

  private static final String DIR_FOR_DOWNLOADS = "/Users/cezary.butler/downloads/gdrive/";

  /** Directory to store user credentials. */
  private static final java.io.File DATA_STORE_DIR =
      new java.io.File(System.getProperty("user.home"), ".store/drive_sample");

  /**
   * Global instance of the {@link DataStoreFactory}. The best practice is to make it a single
   * globally shared instance across your application.
   */
  private static FileDataStoreFactory dataStoreFactory;

  /** Global instance of the HTTP transport. */
  private static HttpTransport httpTransport;

  /** Global instance of the JSON factory. */
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  /** Global Drive API client. */
  private static Drive drive;

  private static AtomicInteger FILE_COUNT = new AtomicInteger();
  private static AtomicDouble FILE_SIZE = new AtomicDouble();

  /** Authorizes the installed application to access user's protected data. */
  private static Credential authorize() throws Exception {
    // load client secrets
    GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY,
        new InputStreamReader(DriveSample.class.getResourceAsStream("/client_secrets.json")));
    if (clientSecrets.getDetails().getClientId().startsWith("Enter")
        || clientSecrets.getDetails().getClientSecret().startsWith("Enter ")) {
      System.out.println(
          "Enter Client ID and Secret from https://code.google.com/apis/console/?api=drive "
              + "into drive-cmdline-sample/src/main/resources/client_secrets.json");
      System.exit(1);
    }
    // set up authorization code flow
    GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
        httpTransport, JSON_FACTORY, clientSecrets,
        Collections.singleton(DriveScopes.DRIVE)).setDataStoreFactory(dataStoreFactory)
        .build();
    // authorize
    final LocalServerReceiver receiver = new LocalServerReceiver();
    return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
  }

  public static void main(String[] args) {
    Preconditions.checkArgument( args.length >= 1);
    final long startTime = System.currentTimeMillis();

    try {
      httpTransport = GoogleNetHttpTransport.newTrustedTransport();
      dataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR);
      // authorization

      System.out.println("Going to authorize");
      Credential credential = authorize();
      // set up the global Drive instance
      drive = new Drive.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName(
          APPLICATION_NAME).build();

      final About about = drive.about().get().setFields("user").execute();
      System.out.println(about.getUser().getDisplayName());
      // run commands

      System.out.println("Listing zoneminder");
      final String zoneminder = "0B3lkE4i92Vu6bTEzeGVkd19qRDQ";

      String pageToken = null;

      final int maxPages = 500;
      int page = 0;
      final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
      final ArrayBlockingQueue<FileList> fileListsQueue = new ArrayBlockingQueue<>(THREAD_COUNT+1);
      final java.io.File targetDirectory = new java.io.File(args[0]);
      for (int i = 0; i < THREAD_COUNT; ++i) {
        final DownloadFilesTask downloadFilesTask = new DownloadFilesTask(drive, targetDirectory, fileListsQueue);
        executorService.submit(downloadFilesTask);
      }
      do {
        System.out.println("Getting another batch of files");
        final FileList fileList = drive.files().list().setPageSize(50)
            .setQ(String
                .format("'%s' in parents and trashed = false and mimeType='application/x-bzip2'",
                    zoneminder))
            .setFields("nextPageToken, files(id, name, size, mimeType)")
            .setPageToken(pageToken)
            .execute();

        fileListsQueue.put(fileList);

        ++page;
        pageToken = fileList.getNextPageToken();

      } while (page <= maxPages && pageToken != null);
      final boolean terminated = executorService.awaitTermination(1, TimeUnit.DAYS);
      if (!terminated) {
        System.out.println("Terminated before finishing tasks");
      }
      System.out
          .printf("Done downloading %d files (total of %f bytes) in %f secons%n", FILE_COUNT.get(),
              FILE_SIZE.get(), (System.currentTimeMillis() - startTime) / 1000.);

      return;
    } catch (IOException e) {
      System.err.println(e.getMessage());
    } catch (Throwable t) {
      t.printStackTrace();
    }
    System.exit(1);
  }

  private static java.io.File createTargetDirectory(java.io.File directory, String dir) {
    final java.io.File target = new java.io.File(directory, dir);
    if (!(target.mkdirs() || target.exists())) {
      System.out.printf("Directory %s was not created%n", target);
    }
    return target;
  }

  static class DownloadFilesTask implements Runnable {

    private final Drive drive;
    private final java.io.File directory;
    private final ArrayBlockingQueue<FileList> fileListQueue;
    private long threadId;

    int fileCount = 0;
    double fileSize = 0;
    private JsonBatchCallback<Void> batchCallback = new JsonBatchCallback<Void>() {
      @Override
      public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) throws IOException {
        System.out.printf("Delete failed: %s%n", e.getMessage());
        e.getErrors().forEach(
            er -> System.err.printf("error: %s%n", er.getMessage())
        );

      }

      @Override
      public void onSuccess(Void aVoid, HttpHeaders responseHeaders) throws IOException {
        System.out.println("Delete executed successfully");
      }
    };

    public DownloadFilesTask(Drive drive, java.io.File directory, ArrayBlockingQueue<FileList> fileListsQueue) {
      this.drive = drive;
      this.directory = directory;
      this.fileListQueue = fileListsQueue;
    }

    private void performDownload(File file, java.io.File targetFile) {
      try (final FileOutputStream fos = new FileOutputStream(targetFile)) {
        final Drive.Files.Get getFile = drive.files().get(file.getId());
        final MediaHttpDownloader mediaHttpDownloader = getFile
            .getMediaHttpDownloader()
            .setDirectDownloadEnabled(false)
            .setProgressListener(downloader -> {
              if (downloader.getDownloadState() == MEDIA_COMPLETE) {
                System.out.printf("%d\tFile %s downloaded, proceeding with delete%n", threadId,
                    file.getName());
                drive.files().delete(file.getId()).execute();
              }
            });
        getFile.executeMediaAndDownloadTo(fos);
      } catch (IOException e) {
        System.out
            .printf("%d\tError occured, the file will be downloaded in next attempt%n", threadId);
        e.printStackTrace();
      }
    }

    final Pattern pattern = Pattern.compile("^ZM([-0-9]+)T.+");

    @Override
    public void run() {
      threadId = Thread.currentThread().getId();
      FileList fileList;
      while (true) {
        try {
          fileList = fileListQueue.poll(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          System.err.printf("Thread %d interrupted%n", threadId);
          Thread.currentThread().interrupt();
          break;
        }
        for (File file : fileList.getFiles()) {
          final String name = file.getName();
          final String mimeType = file.getMimeType();
          final Matcher matcher = pattern.matcher(name);
          fileSize += file.getSize();
          if (matcher.matches()) {
            final String dir = matcher.group(1);
            java.io.File target = createTargetDirectory(directory, dir);
            System.out.printf("%d\tFile #(%4d): %s\t mime: %s - size:%d \t dir: %s%n", threadId,
                fileCount, name, mimeType, file.getSize(), dir);
            final java.io.File targetFile = new java.io.File(target, name.replace(":", "_"));
            if (targetFile.exists()) {
              System.out.printf("%d\tTarget file %s already exists%n", threadId, targetFile);
            }
            performDownload(file, targetFile);
          }

          ++fileCount;
        }

        FILE_COUNT.addAndGet(fileCount);
        FILE_SIZE.addAndGet(fileSize);
      }
    }
  }

}
