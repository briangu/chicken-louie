module Partitions {

  use GenHashKey32;
  
  // Number of dimensions in the partition space.
  // Each partition will be projected to a locale.  
  // If the number of partitions exceeds the number of locales, 
  // then the locales will be over-subscribed with possibly more than one
  // partition per locale.
  config var partitionDimensions = 16;

  // Partition to locale mapping.  Zero-based to allow modulo to work conveniently.
  var Partitions: [0..partitionDimensions-1] locale;

  proc initPartitions() {
    // project the partitions down to the locales
    for i in 0..Partitions.size-1 {
      Partitions[i] = Locales[i % numLocales];
      on Partitions[i] {
        writeln("partition[", i, "] is mapped to locale ", here.id);
      }
    }
  }

  /**
    Map a word to a partition.
  */
  proc partitionForWord(word: string): int {
    return genHashKey32(word) % Partitions.size;
  }

  iter wordPartitions(word: string) {
    yield genHashKey32(word) % Partitions.size;
  }
}