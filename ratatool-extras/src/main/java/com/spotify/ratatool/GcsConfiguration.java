/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.ratatool;

import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase;
import com.google.cloud.hadoop.util.HadoopCredentialConfiguration;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GcsConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(GcsConfiguration.class);

  // ==================================================
  // Ported from google-api-client
  // DefaultCredentialProvider, GoogleCredential
  // ==================================================

  public static Configuration get() {
    Configuration conf = new Configuration();
    File cloudConfigPath;
    if (isWindows()) {
      cloudConfigPath = new File(getEnvironment().get("APPDATA"), "gcloud");
    } else {
      cloudConfigPath = new File(System.getProperty("user.home"), ".config/gcloud");
    }
    File credentialFilePath = new File(cloudConfigPath, "application_default_credentials.json");
    if (!credentialFilePath.exists()) {
      return conf;
    }

    try {
      JsonFactory jsonFactory = Utils.getDefaultJsonFactory();
      InputStream inputStream = new FileInputStream(credentialFilePath);
      JsonObjectParser parser = new JsonObjectParser(jsonFactory);
      GenericJson fileContents = parser.parseAndClose(
          inputStream, Charsets.UTF_8, GenericJson.class);
      String fileType = (String) fileContents.get("type");
      if ("authorized_user".equals(fileType)) {
        String clientId = (String) fileContents.get("client_id");
        String clientSecret = (String) fileContents.get("client_secret");
        if (clientId != null && clientSecret != null) {
          LOG.debug("Using GCP user credential from '{}'", credentialFilePath);
          conf.setIfUnset("fs.gs.impl", GoogleHadoopFileSystem.class.getName());
          conf.setIfUnset("fs.AbstractFileSystem.gs.impl", GoogleHadoopFS.class.getName());
          conf.setIfUnset(GoogleHadoopFileSystemBase.GCS_PROJECT_ID_KEY, defaultProject());
          conf.setIfUnset(GoogleHadoopFileSystemBase.GCS_WORKING_DIRECTORY_KEY, "/hadoop");

          conf.setIfUnset(
              HadoopCredentialConfiguration.BASE_KEY_PREFIX +
              HadoopCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX,
              "false");
          conf.setIfUnset(
              HadoopCredentialConfiguration.BASE_KEY_PREFIX +
              HadoopCredentialConfiguration.CLIENT_ID_SUFFIX,
              clientId);
          conf.setIfUnset(
              HadoopCredentialConfiguration.BASE_KEY_PREFIX +
              HadoopCredentialConfiguration.CLIENT_SECRET_SUFFIX,
              clientSecret);
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to load GCP user credential from '{}'", credentialFilePath);
    }
    return conf;
  }

  // ==================================================
  // Ported from DataflowJavaSDK
  // GcpOptions.DefaultProjectFactory
  // ==================================================

  private static String defaultProject() {
    try {
      File configFile;
      if (getEnvironment().containsKey("CLOUDSDK_CONFIG")) {
        configFile = new File(getEnvironment().get("CLOUDSDK_CONFIG"), "properties");
      } else if (isWindows() && getEnvironment().containsKey("APPDATA")) {
        configFile = new File(getEnvironment().get("APPDATA"), "gcloud/properties");
      } else {
        // New versions of gcloud use this file
        configFile = new File(
            System.getProperty("user.home"),
            ".config/gcloud/configurations/config_default");
        if (!configFile.exists()) {
          // Old versions of gcloud use this file
          configFile = new File(System.getProperty("user.home"), ".config/gcloud/properties");
        }
      }
      String section = null;
      Pattern projectPattern = Pattern.compile("^project\\s*=\\s*(.*)$");
      Pattern sectionPattern = Pattern.compile("^\\[(.*)\\]$");
      for (String line : Files.readLines(configFile, StandardCharsets.UTF_8)) {
        line = line.trim();
        if (line.isEmpty() || line.startsWith(";")) {
          continue;
        }
        Matcher matcher = sectionPattern.matcher(line);
        if (matcher.matches()) {
          section = matcher.group(1);
        } else if (section == null || section.equals("core")) {
          matcher = projectPattern.matcher(line);
          if (matcher.matches()) {
            String project = matcher.group(1).trim();
            LOG.debug("Inferred default GCP project '{}' from gcloud.", project);
            return project;
          }
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to infer default project.", e);
    }
    // return null if can't determine
    return null;
  }

  private static boolean isWindows() {
    return System.getProperty("os.name").toLowerCase(Locale.ENGLISH).contains("windows");
  }

  private static Map<String, String> getEnvironment() {
    return System.getenv();
  }

}