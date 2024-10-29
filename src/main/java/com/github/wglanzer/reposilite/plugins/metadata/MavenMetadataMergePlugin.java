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
import java.util.concurrent.ConcurrentHashMap;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;

/**
 * Main entry point for this reposilite plugin, to merge metadata files based on its mirror upstreams.
 *
 * @author w.glanzer, 07.09.2024
 */
@Plugin(name = "metadata-merge", dependencies = "maven")
public class MavenMetadataMergePlugin extends ReposilitePlugin
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

  /**
   * Cache that contains requests to non existing locations.
   * This gets used to prevent asking the mirrors too often for invalid locations...
   */
  private final Map<Map.Entry<Repository, Location>, Instant> invalidLookupRequestCache = new ConcurrentHashMap<>();

  /**
   * Cache that contains the previously deployed locations.
   * If a location was pre-resolved again, the metadata gets recreated anyways.
   */
  private final Set<Location> deployedLocations = Collections.synchronizedSet(new HashSet<>());

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
          Metadata currentMetadata = pEvent.getRepository().getStorageProvider()
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
              .orNull();

          // if we got a current metadata file already, we can check the validity without scraping the mirrors.
          // This should save us a lot of calculating time.
          if (currentMetadata != null && !validateCoordinates(currentMetadata, pEvent.getGav()))
            return;

          // extract the current groupID
          String currentGroupID = currentMetadata == null ? null : currentMetadata.getGroupId();

          // collect all metadata from mirrors
          for (MirrorHost mirror : pEvent.getRepository().getMirrorHosts())
          {
            List<String> allowedGroups = mirror.getConfiguration().getAllowedGroups();

            // check, if the "allowed groups" are empty or the groupID can not be determined.
            // If both, allowed groups and the groupID, are resolvable, the allowedGroups have to contain the requested ID.
            if (allowedGroups.isEmpty() || currentGroupID == null || allowedGroups.stream().anyMatch(currentGroupID::startsWith))
            {
              Metadata mirrorMetadata = downloadMetadataFromMirror(mirror, pEvent.getGav());
              if (mirrorMetadata != null)
              {
                // validate here too, because we may now be able to resolve all necessary data
                if (!validateCoordinates(mirrorMetadata, pEvent.getGav()))
                  return;

                merger.add(mirrorMetadata);
              }
            }
          }

          // if we got no mirrors, do not merge them
          if (merger.isEmpty())
            return;

          // merge the metadata now
          Metadata mergedMeta = merger.merge(currentMetadata);

          // validate the created metadata to get sure, that we do not store metadata for specific versions
          if (!validateCoordinates(mergedMeta, pEvent.getGav()))
            return;

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

    // Invalidate the requestCache on deploy - the artifacts may be there now
    extensions().registerEvent(DeployEvent.class, pEvent -> {
      invalidLookupRequestCache.clear();

      // proxy all deployed metadata, that should be regenerated afterward
      if (pEvent.getGav().endsWith("maven-metadata.xml"))
        deployedLocations.add(pEvent.getGav());
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
    // If this location was deployed to, we have to regenerate our metadata from scratch
    if (deployedLocations.remove(pGav))
      return true;

    // Try to resolve the real "lastModified" timestamp
    Result<FileTime, ErrorResponse> lastModifiedTimeResult = pRepository.getStorageProvider().getLastModifiedTime(pGav);
    if (lastModifiedTimeResult.isOk())
      // check, if we should try to recaluclate it again
      return lastModifiedTimeResult.get().toInstant()
          .plusSeconds(pRepository.getMetadataMaxAgeInSeconds())
          .isBefore(Instant.now());

    // we did not find the file on our local storage -> lookup in request cache to prevent invalids from spamming to the mirrors
    Instant lastLookupRequest = invalidLookupRequestCache.get(Map.entry(pRepository, pGav));
    if (lastLookupRequest == null || lastLookupRequest
        .plusSeconds(pRepository.getMetadataMaxAgeInSeconds())
        .isBefore(Instant.now()))
    {
      // we did not calculate it, or the entry expired -> cache the timestamp and resolve it
      invalidLookupRequestCache.put(Map.entry(pRepository, pGav), Instant.now());
      return true;
    }

    return false;
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

  /**
   * Validates, if the given {@link Metadata} can be stored under the given location.
   * This is used to get sure, that we do not override any children - only root metadata files, with its links.
   *
   * @param pMetadata Metadata to validate
   * @param pLocation Location to validate against
   * @return true, if the metadata can be stored at the given location
   */
  @SuppressWarnings("BooleanMethodIsAlwaysInverted") // clearer naming
  private boolean validateCoordinates(@NonNls Metadata pMetadata, @NonNls Location pLocation)
  {
    if (pMetadata.getGroupId() == null || pMetadata.getArtifactId() == null)
      return true;

    String metadataRootCoordinates = pMetadata.getGroupId().replace('.', '/') + "/" + pMetadata.getArtifactId() + "/maven-metadata.xml";
    if (!pLocation.equals(Location.of(metadataRootCoordinates)))
    {
      getLogger().info("Metadata of {} can not be refreshed, because it is not the root metadata file", pLocation);
      return false;
    }

    return true;
  }

}
