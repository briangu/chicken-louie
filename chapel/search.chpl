// BUG: declaring use IO, Memory here causes chpl compiler segv

/*

determine number of locales
create map from hash to locale

read a list of words
for each word:
  find hash
  map hash to a locale
  send word to mapped locale

via repl:
  ask for a word
  find hash
  map hash to a locale
  get word info (e.g. count) from locale
  print word info

*/

module Search {
  
  use IO, Memory;

  config const verbose = false;

  type DocId = int(64);

  // for scoring and compactness purposes, consider making this use more contiguous memory,
  //  or an associative array(s) that use the score or docid as index.
  class Entry {
    var word: string;
    var score: real;
    var documentCount: int;
    var documents: [0..8192-1] DocId;
  }

  class PartitionIndex {
    var count: int;
    var entries: [0..(1024*1024/2)-1] Entry; // TODO: use a hashtable once the general approach is validated
    var state$: sync int = 0;
  }

  // number of dimensions in the partition space
  config var partitionDimensions = 16;
  var Partitions: [0..partitionDimensions-1] locale;

  var Indices: [0..Partitions.size-1] PartitionIndex;

  proc initPartitions() {
    // project the partitions down to the locales
    for i in 0..Partitions.size-1 {
      Partitions[i] = Locales[i % numLocales];
      on Partitions[i] {
        writeln("partition[", i, "] is mapped to locale ", here.id);
  
        // allocate the partition index on the partition locale
        Indices[i] = new PartitionIndex();
      }
    }

    writeln();
  }

  proc partitionForWord(word: string): int {
    return 0;
  }

  proc localeForPartition(partition: DocId) {
    if (verbose) then writeln("index: ", partition % Partitions.size);
    return Partitions[partition % Partitions.size];
  }

  proc localeForWord(word: string): locale {
    return localeForPatition(hashFromWord(word));
  }

  proc indexWord(word: string, docid: DocId) {
    var partition = partitionForWord(word);
    var partitionIndex = Indices[partition];
    on partitionIndex {
      // writeln("attempting to get lock");
      var state = partitionIndex.state$;
      // writeln("have lock");

      if (indexContainsWord(word, partitionIndex)) {
        if (verbose) then writeln("adding ", word, " to existing entries on partition ", partition);
        var entry = entryForWord(word, partitionIndex);
        if (entry.documentCount < entry.documents.size) {
          entry.documents[entry.documentCount] = docid;
          entry.documentCount += 1;
        } else {
          if (verbose) then writeln("TODO: realloc documents");
        }
      } else {
        if (verbose) then writeln("adding new entry ", word , " on partition ", partition);

        if (partitionIndex.count < partitionIndex.entries.size) {
          var entry = new Entry();
          entry.word = word;
          entry.score = 0;        
          entry.documents[entry.documentCount] = docid;
          entry.documentCount += 1;

          partitionIndex.entries[partitionIndex.count] = entry;
          partitionIndex.count += 1;
        }
      }

      // writeln("releasing lock");
      partitionIndex.state$ = 0;
      // writeln("released lock");
    }
  }

  proc indexContainsWord(word: string, partitionIndex: PartitionIndex): bool {
    return entryIndexForWord(word, partitionIndex) != -1;
  }

  proc entryForWord(word: string, partitionIndex: PartitionIndex): Entry {
    var entryIndex = entryIndexForWord(word, partitionIndex);
    if (entryIndex >= 0) {
      return partitionIndex.entries[entryIndex];
    }
    return nil;
  }

  proc entryIndexForWord(word: string, partitionIndex: PartitionIndex): int {
    for i in 0..partitionIndex.count-1 {
      if (partitionIndex.entries[i].word == word) {
        return i;
      }
    }
    return -1;
  }

  proc dumpEntry(entry: Entry) {
    on entry {
      writeln("word: ", entry.word, " score: ", entry.score);
      for i in 0..entry.documentCount-1 {
        writeln("\t", entry.documents[i]);
      }
    }
  }

  proc dumpPartition(partition: DocId) {
    var partitionIndex = Indices[partition];
    on partitionIndex {
      writeln("entries on partition (", partition, ") locale (", here.id, ") ", partitionIndex.entries);

      var word: string;
      for i in 0..partitionIndex.count-1 {
        var entry = partitionIndex.entries[i];
        writeln("word: ", entry.word);
        dumpPostingTableForWord(entry.word);
      }
    }
  }

  proc dumpPostingTableForWord(word: string) {
    var partition = partitionForWord(word);
    var partitionIndex = Indices[partition];
    on partitionIndex {
      var entryIndex = entryIndexForWord(word, partitionIndex);
      if (entryIndex >= 0) {
        dumpEntry(partitionIndex.entries[entryIndex]);
      } else {
        writeln("word (", word, ") is not in the index");
      }
    }
  }
}
