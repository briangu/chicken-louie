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
    var word: string; // TODO: remove after debugging
    var data: real;
    var docid: int(64);
    var next: Entry;
  }
  var Words: domain(string);
  var Entries: [Words] Entry;

  // number of dimensions in the partition space
  config var partitionDimensions = 16;
  var Partitions: [0..partitionDimensions-1] locale;

  proc initPartitions() {
    // project the partitions down to the locales
    for i in 0..Partitions.size-1 do
      Partitions[i] = Locales[i % numLocales];

    for i in 0..Partitions.size-1 do
      on Partitions[i] do
        writeln("partition[", i, "] is mapped to locale ", here.id);
    writeln();
  }

  proc partitionForWord(word: string) {
    return 0;
  }

  proc localeForPartition(partition: int(64)) {
    writeln("index: ", partition % Partitions.size);
    return Partitions[partition % Partitions.size];
  }

  proc localeForWord(word: string) {
    return localeForPatition(hashFromWord(word));
  }

  proc indexWord(word: string, docid: int(64)) {
    var partition = partitionForWord(word);
    on localeForPartition(partition) {
      var newEntry = new Entry();
      newEntry.word = word;
      newEntry.docid = docid;

      if (!Words.member(word)) {
        writeln("adding new entry ", word , " on partition ", partition);
//        Words += word;
        newEntry.next = nil;
        Entries[word] = newEntry;
      } else {
        writeln("adding ", word, " to existing entries on partition ", partition);
        newEntry.next = Entries[word];
        Entries[word] = newEntry;
      }
//      writeln("entries on partition ", here.id, " ", Entries);
    }
  }

  // private
  proc dumpEntries(head: Entry) {
    on head {
      var current = head;
      while current {
        writeln("word: ", current.word, " docid: ", current.docid);
        current = current.next;
      }
    }
  }

  proc dumpPartition(partition: int(64)) {
    on localeForPartition(partition) {
      writeln("entries on partition ", here.id, " ", Entries);
      writeln("words on partition ", here.id, " ", Words);

      var word: string;
      for word in Words.sorted() {
        writeln("word: ", word);
        dumpEntries(Entries[word]);
      }
    }
  }

  proc dumpPostingTable(word: string) {
    var partition = partitionForWord(word);
    writeln("using partition ", partition);
    on localeForPartition {
      dumpEntries(Entries[word]);
    }
  }
}


