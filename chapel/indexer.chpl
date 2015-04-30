
module Indexer {

  use Search;
  
  class IndexRequest {
    var word: string;
    var docId: DocId;
  }

  config const buffersize = 1024;
  config const verbose: bool = false;

  var buff$: [0..buffersize-1] sync IndexRequest;
  var bufferIndex: atomic int;

  proc initIndexer() {
    bufferIndex.write(0);

    begin {
      consumer();
    }
  }

  proc enqueueIndexRequest(word: string, docId: DocId) {
    var indexRequest = new IndexRequest(word, docId);
    const idx = nextBufferIndex();
    buff$(idx) = indexRequest;
    if (verbose) then writeln("enqueuing ", indexRequest);
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

  proc haltIndexer() {
    const idx = nextBufferIndex();
    buff$(idx) = nil;
    if (verbose) then writeln("halting consumer");
  }

  proc consumer() {
    for indexRequest in readFromBuff() {
      if (verbose) then writeln("Consumer got: ", indexRequest);
      indexWord(indexRequest.word, indexRequest.docId);
      delete indexRequest;
    }
  }

  iter readFromBuff() {
    var ind = 0,              
        nextVal = buff$(0);

    while (nextVal != nil) {
      yield nextVal;

      ind = (ind + 1) % buffersize;
      nextVal = buff$(ind);
    }
  }
}