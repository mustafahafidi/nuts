var _ = require("lodash");
var Q = require("q");
var path = require("path");
var os = require("os");
var destroy = require("destroy");
var LRU = require("lru-diskcache");
var streamRes = require("stream-res");
var Buffer = require("buffer").Buffer;

const rangeParser = require("range-parser");
const pump = require("pump");
const BRS = require("byte-range-stream");
const ReadableStreamClone = require("readable-stream-clone");
const rangeStream = require("range-stream");

function Backend(nuts, opts) {
  this.cacheId = 0;
  this.nuts = nuts;
  this.opts = _.defaults(opts || {}, {
    // Folder to cache assets
    cache: path.resolve(os.tmpdir(), "nuts"),

    // Cache configuration
    cacheMax: 500 * 1024 * 1024,
    cacheMaxAge: 60 * 60 * 1000
  });

  // Create cache
  this.cache = LRU(opts.cache, {
    max: opts.cacheMax,
    maxAge: opts.cacheMaxAge
  });

  _.bindAll(this);
}

// Memoize a function
Backend.prototype.memoize = function(fn) {
  var that = this;

  return _.memoize(fn, function() {
    return that.cacheId + Math.ceil(Date.now() / that.opts.cacheMaxAge);
  });
};

// New release? clear cache
Backend.prototype.onRelease = function() {
  this.cacheId++;
};

// Initialize the backend
Backend.prototype.init = function() {
  this.cache.init();
  return Q();
};

// List all releases for this repository
Backend.prototype.releases = function() {};

// Return stream for an asset
Backend.prototype.serveAsset = function(asset, req, res) {
  var that = this;
  var cacheKey = asset.id;

  function outputStream(stream) {
    var d = Q.defer();

    const { range } = req.headers;
    if (range) {
      //range request
      return that.streamRangeRes(asset, req, res, stream);
    }

    res.header("Content-Length", asset.size);
    res.attachment(asset.filename);
    streamRes(res, stream, d.makeNodeResolver());
    return d.promise;
  }

  // Key exists
  if (that.cache.has(cacheKey)) {
    return that.cache.getStream(cacheKey).then(outputStream);
  }

  return that.getAssetStream(asset).then(function(stream) {
    return Q.all([
      // Cache the stream
      that.cache.set(cacheKey, stream),

      // Send the stream to the user
      outputStream(stream)
    ]);
  });
};

// Return stream for an asset
Backend.prototype.getAssetStream = function(asset) {};

// Return stream for an asset
Backend.prototype.readAsset = function(asset) {
  return this.getAssetStream(asset).then(function(res) {
    var d = Q.defer();
    var output = Buffer([]);

    function cleanup() {
      destroy(res);
      res.removeAllListeners();
    }

    res
      .on("data", function(buf) {
        output = Buffer.concat([output, buf]);
      })
      .on("error", function(err) {
        cleanup();
        d.reject(err);
      })
      .on("end", function() {
        cleanup();
        d.resolve(output);
      });

    return d.promise;
  });
};

Backend.prototype.streamRangeRes = async function(asset, req, res, stream) {
  const { range } = req.headers;
  const size = asset.size,
    type = undefined; //"text/html";

  // Parse the range header
  let ranges = rangeParser(size, range);

  // Malformed?
  if (ranges === -2) {
    res
      .status(400)
      .type("text")
      .send("Malformed `range` header");
    return;
  }
  // Unsatisfiable?
  let isUnsatisfiable = ranges === -1 || ranges.type !== "bytes";
  if (isUnsatisfiable) {
    res
      .status(416)
      .set("Content-Range", `bytes */${size}`)
      .type("text")
      .send("Range not satisfiable");
    return;
  }
  // For single-range range requests, we don't need to use multipart
  const getStream = async range => {
    /*let clonedStream = stream.tee(); //new ReadableStreamClone(stream);
    clonedStream[0].start = range.start;
    clonedStream[0].end = range.end;*/

    let streamAsset = await (this.cache.has(asset.id)
      ? this.cache.getStream(asset.id)
      : this.getAssetStream(asset));
    return streamAsset.pipe(rangeStream(range.start, range.end));
  };
  const isSingleRange = ranges.length === 1;
  const resolveStream = isSingleRange
    ? getStream(ranges[0])
    : new BRS({
        range,
        getChunk: getStream,
        totalSize: size,
        contentType: type
      });

  // Stream retrieval might be async
  const sourceStream = await resolveStream;

  // Prepare headers to set (only apply them after `beforeSend`, in case we run into trouble)
  let headers;
  if (isSingleRange) {
    headers = {
      "Content-Type": type || "application/octet-stream",
      "Content-Range": `bytes ${ranges[0].start}-${ranges[0].end}/${size}`,
      "Content-Length": 1 + ranges[0].end - ranges[0].start
    };
  } else {
    headers = sourceStream.getHeaders();
  }

  res.set(headers);
  res.status(206);

  // Stream the response!
  pump(sourceStream, res);
};

module.exports = Backend;
