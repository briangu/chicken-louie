// BUG: declaring use IO, Memory here causes chpl compiler segv

module Search {
  
  use Logging, Memory, LockFreeHash, GenHashKey32, Partitions, Time;

  config const sync_writers = false;
  config const entry_size: uint(32) = 1024*1024;
  config const max_doc_node_size: uint(32) =  32 * 1024;

  // TODO: should be using a doc index that maps to a doc id?
  type DocId = uint(64);

  class IndexRequest {
    var word: string;
    var docId: DocId;
  }

  /**
    Document Ids are contained in a linked list, where each node is double the length of the previous node up to max_doc_node_size.
    The linked list HEAD contains the most recently indexed items with the older document ids further down in the list.
    In each node, document ids are added from right to left in the array.
  */
  class DocumentIdNode {
    // controls the size of this document list
    var listSize: int = 1;

    var next: DocumentIdNode;

    // list of documents
    var documents: [0..listSize-1] DocId;

    // number of documents in this node's list
    var documentCount: int;

    // Gets the document id index to use to add a new document id.  documentCount should be incremented after using this index.
    proc documentIdIndex() {
      return documents.size - documentCount - 1;
    }

    proc nextDocumentIdNodeSize() {
      if (documents.size >= max_doc_node_size) {
        return documents.size;
      } else {
        return documents.size * 2;
      }
    }
  }

  // for scoring and compactness purposes, consider making this use more contiguous memory,
  //  or an associative array(s) that use the score or docid as index.
  class Entry {
    var word: string;

    var score: real; // TODO: may not be needed if we use docCount as frequency
    
    // total number of documents in all the documentId nodes
    var documentCount: int;

    var documentIdNode: DocumentIdNode;
  }

  class PartitionIndex {
    var partition: int;
    var entryCount: uint(32);
    var entryIndex = new LockFreeHash(entry_size);
    var entries: [1..entry_size] Entry;

    inline proc lockIndexWriter() {
      // debug("attempting to get lock");
      // if (sync_writers) then while writerLock.testAndSet() do chpl_task_yield();
      // debug("have lock");
    }

    inline proc unlockIndexWriter() {
      // writeln("releasing lock");
      // if (sync_writers) then writerLock.clear();
      // writeln("released lock");
    }

    proc PartitionIndex() {
      partition = 0;
      entryCount += 1; // start at the first index of entries
    }

    proc PartitionIndex(idx: int) {
      partition = idx;
      entryCount += 1; // start at the first index of entries
    }
  }

  var Indices: [0..Partitions.size-1] PartitionIndex;

  proc initIndices() {
    var t: Timer;
    t.start();
    for i in 0..Partitions.size-1 {
      on Partitions[i] {
        info("index [", i, "] is mapped to partition ", i);
  
        // allocate the partition index on the partition locale
        Indices[i] = new PartitionIndex(i);
      }
    }
    t.stop();
    timing("initialized indices in ",t.elapsed(TimeUnits.microseconds), " microseconds");
  }

  proc entryForWord(word: string): Entry {
    var partition = partitionForWord(word);
    var partitionIndex = Indices[partition];
    var entry: Entry;
    on partitionIndex {
      entry = entryForWordOnPartition(word, partitionIndex);
    }
    return entry;
  }

  proc entryForWordOnPartition(word: string, partitionIndex: PartitionIndex): Entry {
    var entryIndex = entryIndexForWord(word, partitionIndex);
    if (entryIndex > 0) {
      return partitionIndex.entries[entryIndex];
    }
    return nil;
  }

  proc indexWord(word: string, docId: DocId) {
    var partition = partitionForWord(word);
    var partitionIndex = Indices[partition];
    on partitionIndex { local {
      partitionIndex.lockIndexWriter();

      indexWordOnPartition(word, docId, partitionIndex);

      partitionIndex.unlockIndexWriter();
    } }
  }

  proc indexWordsOnPartition(requests: [] IndexRequest, requestCount: int, partition: int) {
    var partitionIndex = Indices[partition];
    on partitionIndex { local {
      partitionIndex.lockIndexWriter();

      for i in 0..requestCount-1 {
        indexWordOnPartition(requests[i].word, requests[i].docId, partitionIndex);
      }

      partitionIndex.unlockIndexWriter();
    } }
  }

  proc indexWordOnPartition(word, docId, partitionIndex: PartitionIndex) {
    var entry = entryForWordOnPartition(word, partitionIndex);
    if (entry != nil) {
      debug("adding ", word, " to existing entries on partition ", partitionIndex.partition);
      var docNode = entry.documentIdNode;
      var docCount = docNode.documentCount;
      if (docCount < docNode.documents.size) {
        docNode.documents[docNode.documentIdIndex()] = docId;
        docNode.documentCount += 1;
      } else {
        docNode = new DocumentIdNode(docNode.nextDocumentIdNodeSize(), docNode);
        debug("adding new document id node of size ", docNode.documents.size);
        docNode.documents[docNode.documentIdIndex()] = docId;
        docNode.documentCount = 1;
        entry.documentIdNode = docNode;
      }
      entry.documentCount += 1;
    } else {
      debug("adding new entry ", word , " on partition ", partitionIndex.partition);

      var entriesCount = partitionIndex.entryCount;
      if (entriesCount < partitionIndex.entries.size) {
        entry = new Entry();
        entry.word = word;
        entry.score = 0;        

        var docNode = new DocumentIdNode();
        docNode.documents[docNode.documentIdIndex()] = docId;
        docNode.documentCount = 1;
        entry.documentIdNode = docNode;
        entry.documentCount = 1;

        var entryIndex: uint(32) = partitionIndex.entryCount;
        partitionIndex.entryCount += 1;
        partitionIndex.entries[entryIndex] = entry;
        var success = partitionIndex.entryIndex.setItem(word, entryIndex);
        if (!success) {
          error("indexWord: failed to index ", word);
          exit(0);
          // how do we accumuate per-partition indexing errors for a final response?
        }
      }
    }
  }

  proc indexContainsWord(word: string, partitionIndex: PartitionIndex): bool {
    return entryIndexForWord(word, partitionIndex) != 0;
  }

  proc entryIndexForWord(word: string, partitionIndex: PartitionIndex): uint(32) {
    return partitionIndex.entryIndex.getItem(word);
  }

  iter documentIdsForEntry(entry: Entry) {
    var node = entry.documentIdNode;
    while (node != nil) {
      var startIdx = node.documents.size - node.documentCount;
      for i in startIdx..node.documents.size-1 {
        var docId = node.documents[i];
        if (docId > 0) {
          yield docId;
        }
      }
      node = node.next;
    }
  }

  // can't yield inside on in 1.11.0
  // 
  // iter documentIdsForWord(word: string) {
  //   var entry = entryForWord(word);
  //   if (entry != nil) {
  //     on entry {
  //       var node = entry.documentIdNode;
  //       while (node != nil) {
  //         var startIdx = node.documents.size - node.documentCount.read();
  //         for i in startIdx..node.documents.size-1 {
  //           var docId = node.documents[i];
  //           if (docId > 0) {
  //             yield docId;
  //           }
  //         }
  //         node = node.next;
  //       }
  //     }
  //   }
  // }

  proc dumpEntry(entry: Entry) {
    on entry {
      info("word: ", entry.word, " score: ", entry.score);
      var count = 0;
      for docId in documentIdsForEntry(entry) {
        writeln("\t", docId);
        count += 1;
      }
      if (count != entry.documentCount) {
        error("ERROR: documentCount != count", count, entry.documentCount);
      }
    }
  }

  proc dumpPartition(partition: int) {
    var partitionIndex = Indices[partition];
    on partitionIndex {
      info("entries on partition (", partition, ") locale (", here.id, ") ", partitionIndex.entries);

      var word: string;
      for i in 0..partitionIndex.entryCount-1 {
        var entry = partitionIndex.entries[i];
        info("word: ", entry.word);
        dumpPostingTableForWord(entry.word);
      }
    }
  }

  proc dumpPostingTableForWord(word: string) {
    var entry = entryForWord(word);
    if (entry != nil) {
      dumpEntry(entry);
    } else {
      error("word (", word, ") is not in the index");
    }
  }
}
