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

  // for scoring and compactness purposes, consider making this use more contiguous memory,
  //  or an associative array(s) that use the score or docid as index.
  class Entry {
    var word: string;
    var score: real(64);
    var documentCount: atomic int;
    var documents: [1..1024] int(64);
  }

  class PartitionIndex {
    var count: atomic int;
    var words: domain(string);
    var entries: [1..1024] Entry;
    var state$: sync int = 0;
  }

  // number of dimensions in the partition space
  config var partitionDimensions = 16;
  var Partitions: [0..partitionDimensions-1] locale;

  var Indices: [1..Partitions.size] PartitionIndex;

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
    return 1;
  }

  proc localeForPartition(partition: int(64)) {
    writeln("index: ", partition % Partitions.size);
    return Partitions[partition % Partitions.size];
  }

  proc localeForWord(word: string): locale {
    return localeForPatition(hashFromWord(word));
  }

  proc indexWord(word: string, docid: int(64)) {
    var partition = partitionForWord(word);
    var partitionIndex = Indices[partition];
    on partitionIndex {

      var state = partitionIndex.state$;

      if (!partitionIndex.words.member(word)) {
        writeln("adding new entry ", word , " on partition ", partition);
        partitionIndex.words += word;

        var entry = new Entry();
        entry.word = word;
        entry.score = 0;        
        entry.documents[entry.documentCount.read()] = docid;
        entry.documentCount.add(1);

        partitionIndex.entries[partitionIndex.count.read()] = entry;
        partitionIndex.count.add(1);
      } else {
        writeln("adding ", word, " to existing entries on partition ", partition);
        var entryIndex = entryIndexForWord(word, partitionIndex);
        var entry = partitionIndex.entries[entryIndex];
        entry.documents[entry.documentCount.read()] = docid;
        entry.documentCount.add(1);
      }

      partitionIndex.state$ = 0;
    }
  }

  proc entryIndexForWord(word: string, partitionIndex: PartitionIndex): int {
    for i in 1..partitionIndex.count.read() {
      if (partitionIndex.entries[i].word == word) {
        return i;
      }
    }
    return -1;
  }

  proc dumpEntry(entry: Entry) {
    on entry {
      writeln("word: ", entry.word, " score: ", entry.score);
      for i in 1..entry.documentCount.read() {
        writeln("\t", entry.documents[i]);
      }
    }
  }

  proc dumpPartition(partition: int(64)) {
    var partitionIndex = Indices[partition];
    on partitionIndex {
      writeln("entries on partition ", here.id, " ", partitionIndex.entries);
      writeln("words on partition ", here.id, " ", partitionIndex.words);

      var word: string;
      for word in partitionIndex.words.sorted() {
        writeln("word: ", word);
        dumpPostingTableForWord(word);
      }
    }
  }

  proc dumpPostingTableForWord(word: string) {
    var partition = partitionForWord(word);
    var partitionIndex = Indices[partition];
    on partitionIndex {
      dumpEntry(partitionIndex.entries[entryIndexForWord(word, partitionIndex)]);
    }
  }
}
