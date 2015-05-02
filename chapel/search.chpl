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
  
  use IO, Memory, LockFreeHash, GenHashKey32;

  config const verbose = false;
  config const sync_writers = false;
  config const entry_size: uint(32) = 1024*1024;

  type DocId = uint(64);

  // for scoring and compactness purposes, consider making this use more contiguous memory,
  //  or an associative array(s) that use the score or docid as index.
  class Entry {
    var word: string;
    var score: real; // TODO: may not be needed if we use docCount as frequency
    var documentCount: atomic int;
    var documents: [0..8192-1] DocId; // TODO: point to the tail of a linked list; keep track of node + index into node
  }

  class PartitionIndex {
    var entryCount: atomic uint(32);
    var entryIndex = new LockFreeHash(uint(32), entry_size);
    var entries: [1..entry_size] Entry;
    var writerLock: atomicflag;

    inline proc lockIndexWriter() {
      // writeln("attempting to get lock");
      if (sync_writers) then while writerLock.testAndSet() do chpl_task_yield();
      // writeln("have lock");
    }

    inline proc unlockIndexWriter() {
      // writeln("releasing lock");
      if (sync_writers) then writerLock.clear();
      // writeln("released lock");
    }
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
      partitionIndex.lockIndexWriter();

      var entry = entryForWord(word, partitionIndex);
      if (entry != nil) {
        if (verbose) then writeln("adding ", word, " to existing entries on partition ", partition);
        var docCount = entry.documentCount.read();
        if (docCount < entry.documents.size) {
          entry.documents[docCount] = docid;
          entry.documentCount.add(1);
        } else {
          // TODO: append node to linked list of document ids
          if (verbose) then writeln("TODO: realloc documents");
        }
      } else {
        if (verbose) then writeln("adding new entry ", word , " on partition ", partition);

        var entriesCount = partitionIndex.entryCount.read();
        if (entriesCount < partitionIndex.entries.size) {
          entry = new Entry();
          entry.word = word;
          entry.score = 0;        
          entry.documents[0] = docid;
          entry.documentCount.add(1);

          var entryIndex: uint(32) = partitionIndex.entryCount.fetchAdd(1);
          partitionIndex.entries[entryIndex] = entry;
          partitionIndex.entryIndex.setItem(genHashKey32(word), entryIndex);
//          partitionIndex.entryCount.add(1);
        }
      }

      partitionIndex.unlockIndexWriter();
    }
  }

  proc indexContainsWord(word: string, partitionIndex: PartitionIndex): bool {
    return entryIndexForWord(word, partitionIndex) != 0;
  }

  proc entryForWord(word: string, partitionIndex: PartitionIndex): Entry {
    var entryIndex = entryIndexForWord(word, partitionIndex);
    if (entryIndex > 0) {
      return partitionIndex.entries[entryIndex];
    }
    return nil;
  }

  proc entryIndexForWord(word: string, partitionIndex: PartitionIndex): uint(32) {
    return partitionIndex.entryIndex.getItem(genHashKey32(word));
  }

  proc dumpEntry(entry: Entry) {
    on entry {
      writeln("word: ", entry.word, " score: ", entry.score);
      for i in 0..entry.documentCount.read()-1 {
        writeln("\t", entry.documents[i]);
      }
    }
  }

  proc dumpPartition(partition: int) {
    var partitionIndex = Indices[partition];
    on partitionIndex {
      writeln("entries on partition (", partition, ") locale (", here.id, ") ", partitionIndex.entries);

      var word: string;
      for i in 0..partitionIndex.entryCount.read()-1 {
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
      if (entryIndex > 0) {
        dumpEntry(partitionIndex.entries[entryIndex]);
      } else {
        writeln("word (", word, ") is not in the index");
      }
    }
  }
}
