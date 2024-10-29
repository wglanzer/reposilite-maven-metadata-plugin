package com.github.wglanzer.reposilite.plugins.metadata;

import com.reposilite.maven.api.*;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.jetbrains.annotations.*;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * Class that can merge multiple {@link Metadata} instances into a single one
 *
 * @author w.glanzer, 07.09.2024
 */
class MavenMetadataMerger
{
  private static final SimpleDateFormat LAST_UPDATED_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
  private final Set<Metadata> metadataSet = new HashSet<>();

  /**
   * Adds the given {@link Metadata} to the list of metadata to merge
   *
   * @param pMetadata metadata that should be merged
   */
  public synchronized void add(@NonNls Metadata pMetadata)
  {
    metadataSet.add(pMetadata);
  }

  /**
   * @return true, if the backing list is empty and nothing is about to be merged
   */
  public synchronized boolean isEmpty()
  {
    return metadataSet.isEmpty();
  }

  /**
   * Merges the previously given metadata files into a single one,
   * so the returned contains the correct information about all of it
   *
   * @param pBaseMetadata {@link Metadata} that acts as a "base" and should be a proxy repository metadata.
   *                      This does not get asked for release/latest versions, because it is always newer than our base repositories.
   * @return the merged {@link Metadata}
   * @see #add(Metadata)
   */
  @NonNls
  public synchronized Metadata merge(@Nullable Metadata pBaseMetadata)
  {
    String groupId;
    String artifactId;
    AtomicReference<String> releaseRef = new AtomicReference<>(null);
    AtomicReference<String> latestRef = new AtomicReference<>(null);
    AtomicReference<String> lastUpdatedRef = new AtomicReference<>();
    List<String> versions;
    List<Plugin> plugins;

    // Update Release / Latest
    metadataSet.stream()
        .map(Metadata::getVersioning)
        .filter(Objects::nonNull)
        .max(Comparator.comparing(pVersioning -> {
          try
          {
            return LAST_UPDATED_FORMAT.parse(pVersioning.getLastUpdated());
          }
          catch (Exception e)
          {
            return new Date(0);
          }
        }))
        .ifPresentOrElse(pV -> {
          releaseRef.set(pV.getRelease());
          latestRef.set(pV.getRelease());
          lastUpdatedRef.set(pV.getLastUpdated());
        }, () -> lastUpdatedRef.set(LAST_UPDATED_FORMAT.format(new Date())));

    // Add the base metadata now, because we now know the release/latest versions
    if (pBaseMetadata != null)
      metadataSet.add(pBaseMetadata);

    // Find groupId and artifactId
    groupId = metadataSet.stream()
        .map(Metadata::getGroupId)
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(null);
    artifactId = metadataSet.stream()
        .map(Metadata::getArtifactId)
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(null);

    // Update Versions
    versions = new ArrayList<>();
    for (Metadata mergeMeta : metadataSet)
      Stream.ofNullable(mergeMeta.getVersioning())
          .filter(Objects::nonNull)
          .map(Versioning::getVersions)
          .filter(Objects::nonNull)
          .flatMap(Collection::stream)
          .filter(pV -> !versions.contains(pV))
          .forEach(versions::add);
    versions.sort(Comparator.comparing(ComparableVersion::new));

    // Update Plugins
    plugins = new ArrayList<>();
    for (Metadata mergeMeta : metadataSet)
      Stream.ofNullable(mergeMeta.getPlugins())
          .filter(Objects::nonNull)
          .flatMap(List::stream)
          .filter(Objects::nonNull)
          .filter(pPlugin -> plugins.stream()
              .noneMatch(pAlreadyExistingPlugin -> Objects.equals(pAlreadyExistingPlugin.getPrefix(), pPlugin.getPrefix())))
          .forEach(plugins::add);

    // Create a new Metadata instance and return
    return new Metadata(groupId, artifactId, null, new Versioning(releaseRef.get(), latestRef.get(), versions, null, null, lastUpdatedRef.get()),
                        plugins);
  }

}
