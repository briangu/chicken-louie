module Indexer {

  use Logging, Partitions, Search;
  
  class IndexRequest {
    var word: string;
    var docId: DocId;
  }

  config const buffersize = 1024;
  config const testAfterIndex: bool = true;

  class PartitionIndexer {
    var buff$: [0..buffersize-1] sync IndexRequest;
    var bufferIndex: atomic int;
    var release$: single bool;

    proc PartitionIndexer() {
      // via nextBufferIndex, this is incremented to zero before first use
      bufferIndex.write(-1);
    }

    proc startConsumer() {
      begin {
        consumer();
      }
    }

    proc nextBufferIndex(): int {
      var idx: int;
      var success = false;
      while (!success) { 
        var originalValue = bufferIndex.read();
        idx = (originalValue + 1) % buffersize;
        success = bufferIndex.compareExchange(originalValue, idx);
      }
      return idx;
    }

    proc enqueueIndexRequest(word: string, docId: DocId) {
      var indexRequest = new IndexRequest(word, docId);
      const idx = nextBufferIndex();
      buff$(idx).writeEF(indexRequest);
      debug("enqueuing ", indexRequest);
    }

    proc waitForIndexer() {
      debug("waiting...");
      release$;
      debug("done waiting...");
    }

    proc markCompleteForIndexer() {
      debug("marking for completion");
      const idx = nextBufferIndex();
      buff$(idx).writeEF(nil);
      debug("halting consumer");
    }

    proc consumer() {
      for indexRequest in readFromBuff() {
        debug("Indexing: start ", indexRequest, "...");
        indexWord(indexRequest.word, indexRequest.docId);
        if (testAfterIndex) {
          var entry = entryForWord(indexRequest.word);
          if (entry == nil || entry.word != indexRequest.word) {
            error("indexer: failed to index word ", indexRequest.word);
            exit(0);
          }
        }
        debug("Indexing: complete ", indexRequest, "...");
        delete indexRequest;
      }
    }

    iter readFromBuff() {
      var ind = 0;
      var nextVal = buff$(ind);

      while (nextVal != nil) {
        yield nextVal;

        ind = (ind + 1) % buffersize;
        nextVal = buff$(ind);
      }

      release$ = true;
    }
  }

  var indexers: [0..Partitions.size-1] PartitionIndexer;

  proc initIndexer() {
    for i in 0..Partitions.size-1 {
      on Partitions[i] {
        indexers[i] = new PartitionIndexer();
        indexers[i].startConsumer();
      }
    }
  }

  proc indexerForWord(word: string): PartitionIndexer {
    return indexers[partitionForWord(word)];
  }

  proc enqueueIndexRequest(word: string, docId: DocId) {
    var indexRequest = new IndexRequest(word, docId);
    debug("enqueuing ", indexRequest);
    var indexer = indexerForWord(word);
    // TODO: do we need to go onto the indexer locale for this?  or will it just automatically be on that locale?
    on indexer {
      indexer.enqueueIndexRequest(word, docId);
    }
  }

  proc waitForIndexer() {
    markCompleteForIndexer();
    debug("waiting...");
    for indexer in indexers {
      // TODO: do we need to do this on the locale? or can we just call waitForIndexer and have it work?
      on indexer {
        indexer.waitForIndexer();
      }
    }
    debug("done waiting...");
  }

  proc markCompleteForIndexer() {
    debug("marking for completion");
    for indexer in indexers {
      // TODO: do we need to do this on the locale? or can we just call waitForIndexer and have it work?
      on indexer {
        indexer.markCompleteForIndexer();
      }
    }
    debug("halting consumer");
  }
}