package com.github.wglanzer.reposilite.plugins.metadata;

import com.reposilite.maven.api.*;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.jetbrains.annotations.NonNls;

import java.text.SimpleDateFormat;
import java.util.*;
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
   * Adds the given {@link Metadata} to the list of metadatas to merge
   *
   * @param pMetadata metadata that should be merged
   */
  public void add(@NonNls Metadata pMetadata)
  {
    metadataSet.add(pMetadata);
  }

  /**
   * @return true, if the backing list is empty and nothing is about to be merged
   */
  public boolean isEmpty()
  {
    return metadataSet.isEmpty();
  }

  /**
   * Merges the previously given metadata files into a single one,
   * so the returned contains the correct information about all of it
   *
   * @return the merged {@link Metadata}
   * @see #add(Metadata)
   */
  @NonNls
  public Metadata merge()
  {
    String groupId;
    String artifactId;
    String release;
    String latest;
    List<String> versions;
    Snapshot snapshot = null;
    List<SnapshotVersion> snapshotVersions = null;
    String lastUpdated = LAST_UPDATED_FORMAT.format(new Date());

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

    // Update Release / Latest
    release = metadataSet.stream()
        .map(Metadata::getVersioning)
        .filter(Objects::nonNull)
        .map(Versioning::getRelease)
        .filter(Objects::nonNull)
        .max(Comparator.comparing(ComparableVersion::new))
        .orElse(null);
    latest = metadataSet.stream()
        .map(Metadata::getVersioning)
        .filter(Objects::nonNull)
        .map(Versioning::getLatest)
        .filter(Objects::nonNull)
        .max(Comparator.comparing(ComparableVersion::new))
        .orElse(null);

    //todo snapshot / snapshotVersions
    //snapshot = originalVersioning == null ? null : originalVersioning.getSnapshot();
    //snapshotVersions = originalVersioning == null ? null : originalVersioning.getSnapshotVersions();

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

    // Create a new Metadata instance and return
    return new Metadata(groupId, artifactId, null, new Versioning(release, latest, versions, snapshot, snapshotVersions, lastUpdated), null);
  }

}
