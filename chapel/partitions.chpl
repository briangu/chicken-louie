module Partitions {
  
  use GenHashKey32;

    // number of dimensions in the partition space
  config var partitionDimensions = 16;
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

  proc partitionForWord(word: string): int {
    return genHashKey32(word) % Partitions.size;
  }

  proc localeForPartition(partition: int) {
    return Partitions[partition % Partitions.size];
  }

  proc localeForWord(word: string): locale {
    return localeForPartition(hashFromWord(word));
  }
}