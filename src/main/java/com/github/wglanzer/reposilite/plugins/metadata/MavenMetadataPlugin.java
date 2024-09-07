package com.github.wglanzer.reposilite.plugins.metadata;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.dataformat.xml.*;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import com.fasterxml.jackson.module.kotlin.KotlinModule;
import com.reposilite.maven.*;
import com.reposilite.maven.api.*;
import com.reposilite.plugin.api.Plugin;
import com.reposilite.plugin.api.*;
import com.reposilite.shared.ErrorResponse;
import com.reposilite.storage.api.Location;
import org.jetbrains.annotations.*;
import panda.std.Result;

import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.*;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;

/**
 * @author w.glanzer, 07.09.2024
 */
@Plugin(name = "metadata", dependencies = "maven")
public class MavenMetadataPlugin extends ReposilitePlugin
{
  private static final XmlMapper XML_MAPPER = XmlMapper.xmlBuilder()
      .addModules(new JacksonXmlModule(), new KotlinModule.Builder().build())
      .configure(ToXmlGenerator.Feature.WRITE_XML_DECLARATION, true)
      .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .defaultUseWrapper(false)
      .enable(INDENT_OUTPUT)
      .build();

  private final Set<PreResolveEvent> processingEvents = Collections.synchronizedSet(new HashSet<>());

  @Nullable
  @Override
  public Facade initialize()
  {
    extensions().registerEvent(PreResolveEvent.class, pEvent -> {

      // Prevent multiple events for the same file at the same time
      if (!processingEvents.add(pEvent))
        return;

      try
      {
        long startTimestamp = System.currentTimeMillis();

        // Only trigger on metadata files and only if we should store something
        if (!pEvent.getRepository().getMirrorHosts().isEmpty() && pEvent.getGav().endsWith("maven-metadata.xml"))
        {
          // Check, if the metadata should be regenerated
          if (!shouldRegenerateMetadata(pEvent.getRepository(), pEvent.getGav()))
          {
            getLogger().info("Skipping generation of maven-metadata.xml for {}, because the file did not reach its end of life.", pEvent.getGav());
            return;
          }

          MavenMetadataMerger merger = new MavenMetadataMerger();

          // find current metadata on local storage
          pEvent.getRepository().getStorageProvider()
              .getFile(pEvent.getGav())
              .map(pStream -> {
                try
                {
                  return XML_MAPPER.readValue(pStream, Metadata.class);
                }
                catch (Exception e)
                {
                  getLogger().warn("Local maven-metadata.xml seems to be invalid, because it could not be parsed");
                  return null;
                }
              })
              .peek(merger::add);

          // collect all metadata from mirrors
          for (MirrorHost mirror : pEvent.getRepository().getMirrorHosts())
          {
            Metadata mirrorMetadata = downloadMetadataFromMirror(mirror, pEvent.getGav());
            if (mirrorMetadata != null)
              merger.add(mirrorMetadata);
          }

          // if we got no mirrors, do not merge them
          if (merger.isEmpty())
            return;

          // merge the metadata now
          Metadata mergedMeta = merger.merge();

          // upload the merged file
          extensions().facade(MavenFacade.class).saveMetadata(new SaveMetadataRequest(pEvent.getRepository(), pEvent.getGav(), mergedMeta));

          // log, that we did something
          getLogger().info("Regenerated maven-metadata.xml for {} in {} ms", pEvent.getGav(), System.currentTimeMillis() - startTimestamp);
        }
      }
      finally
      {
        processingEvents.remove(pEvent);
      }
    });

    return null;
  }

  /**
   * Calculates, if the metadata file should be regenerated or not, if it is already up-to-date.
   *
   * @param pRepository Repository to check the file
   * @param pGav        Location, which file to search
   * @return true, if the metadata file should be regenerated
   */
  private boolean shouldRegenerateMetadata(@NonNls Repository pRepository, @NonNls Location pGav)
  {
    Result<FileTime, ErrorResponse> lastModifiedTimeResult = pRepository.getStorageProvider().getLastModifiedTime(pGav);
    if (lastModifiedTimeResult.isOk())
      return lastModifiedTimeResult.get().toInstant()
          .plusSeconds(pRepository.getMetadataMaxAgeInSeconds())
          .isBefore(Instant.now());

    // we did not find the file -> regenerate
    return true;
  }

  /**
   * Downloads and parses the metadata file from the given mirror
   *
   * @param pMirror Mirror to download files from
   * @param pGav    Location that should be downloaded
   * @return the downloaded and parsed metadata, or null if not found
   */
  @Nullable
  private Metadata downloadMetadataFromMirror(@NonNls MirrorHost pMirror, @NonNls Location pGav)
  {
    // Create the URL to download from.
    // It should not matter, if we are a local loop back mirror or a real webserver
    String locationToDownload = pMirror.getHost();
    if (!locationToDownload.endsWith("/"))
      locationToDownload += "/";
    locationToDownload += pGav;

    // Download from mirror
    return pMirror.getClient()
        .get(locationToDownload, pMirror.getConfiguration().getAuthorization(), pMirror.getConfiguration().getConnectTimeout(), pMirror.getConfiguration().getReadTimeout())
        .map(pStream -> {
          try
          {
            return XML_MAPPER.readValue(pStream, Metadata.class);
          }
          catch (Exception e)
          {
            getLogger().warn("Failed to parse maven-metadata.xml for {} from mirror {}", pGav, pMirror.getHost());
            return null;
          }
        })
        .orNull();
  }

}