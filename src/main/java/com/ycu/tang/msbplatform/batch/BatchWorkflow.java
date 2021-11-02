package com.ycu.tang.msbplatform.batch;

import backtype.cascading.tap.PailTap;
import backtype.cascading.tap.PailTap.PailTapOptions;
import backtype.hadoop.pail.Pail;
import backtype.hadoop.pail.Pail.TypedRecordOutputStream;
import backtype.hadoop.pail.PailSpec;
import backtype.hadoop.pail.PailStructure;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascalog.ops.IdentityBuffer;
import cascalog.ops.RandLong;
import com.clojurewerkz.cascading.mongodb.MongoDBScheme;
import com.clojurewerkz.cascading.mongodb.MongoDBTap;
import com.ycu.tang.msbplatform.batch.function.*;
import com.ycu.tang.msbplatform.service.PailService;
import elephantdb.DomainSpec;
import elephantdb.jcascalog.EDB;
import elephantdb.partition.HashModScheme;
import elephantdb.persistence.JavaBerkDB;
import jcascalog.Api;
import jcascalog.Fields;
import jcascalog.Option;
import jcascalog.Subquery;
import jcascalog.op.Count;
import jcascalog.op.Sum;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.*;

import com.ycu.tang.msbplatform.service.pailstructure.DataPailStructure;
import com.ycu.tang.msbplatform.service.pailstructure.SplitDataPailStructure;
import com.ycu.tang.msbplatform.batch.thrift.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.ycu.tang.msbplatform.batch.test.Data.*;

/**
 * The entire batch layer for SuperWebAnalytics.com. This is a purely recomputation
 * based implementation. Additional efficiency can be achieved by adding an 
 * incremental batch layer as discussed in Chapter 18.
 */
@Component
public class BatchWorkflow {

    @Autowired
    protected Properties properties;

    @Autowired
    protected PailService pailService;

    public String ROOT = null;
    public String DATA_ROOT = null;
    public String OUTPUTS_ROOT = null;
    public String MASTER_ROOT = null;
    public String NEW_ROOT = null;

    public void init(){
        ROOT = properties.getRoot();
        DATA_ROOT = properties.getDataRoot();
        OUTPUTS_ROOT = properties.getOutputsRoot();
        MASTER_ROOT = properties.getMasterRoot();
        NEW_ROOT = properties.getNewRoot();
    }

    public void initTestData() throws Exception {
        FileSystem fs = pailService.getFs();
        fs.delete(new Path(DATA_ROOT), true);
        fs.delete(new Path(OUTPUTS_ROOT), true);
        fs.delete(new Path(NEW_ROOT), true);
        fs.mkdirs(new Path(DATA_ROOT));
        fs.mkdirs(new Path(OUTPUTS_ROOT + "edb"));

//        Pail masterPail = Pail.create(MASTER_ROOT, (PailStructure)new SplitDataPailStructure());
        Pail<Data> newPail = pailService.createPail(NEW_ROOT, (PailStructure)new DataPailStructure());

        TypedRecordOutputStream os = newPail.openWrite();
        os.writeObject(makePageview(1, "http://foo.com/post1", 60));
        os.writeObject(makePageview(3, "http://foo.com/post1", 62));
        os.writeObject(makePageview(1, "http://foo.com/post1", 4000));
        os.writeObject(makePageview(1, "http://foo.com/post2", 4000));
        os.writeObject(makePageview(1, "http://foo.com/post2", 10000));
        os.writeObject(makePageview(5, "http://foo.com/post3", 10600));
        os.writeObject(makeEquiv(1, 3));
        os.writeObject(makeEquiv(3, 5));

        os.writeObject(makePageview(2, "http://foo.com/post1", 60));
        os.writeObject(makePageview(2, "http://foo.com/post3", 62));

        os.close();

    }


    public void setApplicationConf() {
      Map conf = new HashMap();
      String sers = "backtype.hadoop.ThriftSerialization";
      sers += ",";
      sers += "org.apache.hadoop.io.serializer.WritableSerialization";
      conf.put("io.serializations", sers);
      Api.setApplicationConf(conf);
    }

    public PailTap attributeTap(
            String path,
            final DataUnit._Fields... fields) {
        PailTapOptions opts = new PailTapOptions();

        List list = new ArrayList();
        for(DataUnit._Fields field: fields) {
            list.add("" + field.getThriftFieldId());
        }

        opts.attrs = new List[] {list};
        opts.spec = new PailSpec(
                      (PailStructure) new SplitDataPailStructure());

        return new PailTap(properties.getNamenodeUrl() + path, opts);
    }

    public PailTap splitDataTap(String path) {
        PailTapOptions opts = new PailTapOptions();
        opts.spec = new PailSpec(
                      (PailStructure) new SplitDataPailStructure());
        return new PailTap(properties.getNamenodeUrl() + path, opts);
    }

    public PailTap dataTap(String path) {
        PailTapOptions opts = new PailTapOptions();
        opts.spec = new PailSpec(
                      (PailStructure) new DataPailStructure());
        return new PailTap(properties.getNamenodeUrl() + path, opts);
    }


    public void appendNewDataToMasterDataPail(Pail masterPail,
            Pail snapshotPail) throws IOException {
        Pail shreddedPail = shred();
        masterPail.absorb(shreddedPail);
    }

    public void ingest(Pail masterPail, Pail newDataPail)
            throws IOException {
        FileSystem fs = FileSystem.get(pailService.getHadoopConf());
        fs.delete(new Path("/tmp/swa"), true);
        fs.mkdirs(new Path("/tmp/swa"));

        Pail snapshotPail = newDataPail.snapshot(
                properties.getNamenodeUrl() + "/tmp/swa/newDataSnapshot");
        appendNewDataToMasterDataPail(masterPail, snapshotPail);
        newDataPail.deleteSnapshot(snapshotPail);
    }


    public Pail shred() throws IOException {
        PailTap source = dataTap("/tmp/swa/newDataSnapshot");
        PailTap sink = splitDataTap("/tmp/swa/shredded");

        Subquery reduced = new Subquery("?rand", "?data")
            .predicate(source, "_", "?data-in")
            .predicate(new RandLong(), "?rand")
            .predicate(new IdentityBuffer(), "?data-in").out("?data");

        Api.execute(
            sink,
            new Subquery("?data")
                .predicate(reduced, "_", "?data"));
        Pail shreddedPail = pailService.getPail("/tmp/swa/shredded");
        shreddedPail.consolidate();
        return shreddedPail;
    }

    public void normalizeURLs() {
        Tap masterDataset = splitDataTap(DATA_ROOT + "master");
        Tap outTap = splitDataTap("/tmp/swa/normalized_urls");

        Api.execute(outTap,
            new Subquery("?normalized")
                .predicate(masterDataset, "_", "?raw")
                .predicate(new NormalizeURL(), "?raw")
                    .out("?normalized"));
    }

    public void deduplicatePageviews() {
        Tap source = attributeTap(
                        "/tmp/swa/normalized_pageview_users",
                        DataUnit._Fields.PAGE_VIEW);
        Tap outTap = splitDataTap("/tmp/swa/unique_pageviews");

        Api.execute(outTap,
              new Subquery("?data")
                  .predicate(source, "_", "?data")
                  .predicate(Option.DISTINCT, true));
    }

    public Subquery pageviewBatchView() {
        Tap source = splitDataTap("/tmp/swa/unique_pageviews");

        Subquery hourlyRollup = new Subquery(
            "?url", "?hour-bucket", "?count")
            .predicate(source, "_", "?pageview")
            .predicate(new ExtractPageViewFields(), "?pageview")
                .out("?url", "?person", "?timestamp")
            .predicate(new ToHourBucket(), "?timestamp")
                .out("?hour-bucket")
            .predicate(new Count(), "?count");

        return new Subquery(
            "?url", "?granularity", "?bucket", "?total-pageviews")
            .predicate(hourlyRollup, "?url", "?hour-bucket", "?count")
            .predicate(new EmitGranularities(), "?hour-bucket")
                .out("?granularity", "?bucket")
            .predicate(new Sum(), "?count").out("?total-pageviews");
    }


    public void pageviewMongoDB(Subquery pageviewBatchView) {
        Subquery toMongoDB =
                new Subquery("?key", "?value", "?url", "?granularity", "?bucket")
                        .predicate(pageviewBatchView,
                                "?url", "?granularity", "?bucket", "?value")
                        .predicate(new ToUrlBucketedKey(),
                                "?url", "?granularity", "?bucket")
                        .out("?key");


        // List of columns to be fetched from Mongo
        List<String> columns = new ArrayList<String>();
        columns.add("key");
        columns.add("value");
        columns.add("url");
        columns.add("granularity");
        columns.add("bucket");

        // When writing back to mongodb, you may have Cascading output tuple item names
        // a bit different from your Mongodb ColumnFamily definition. Otherwise, you can
        // simply specify both key and value same.
        Map<String, String> mappings = new HashMap<String, String>();
        mappings.put("key", "?key");
        mappings.put("value", "?value");
        mappings.put("url", "?url");
        mappings.put("granularity", "?granularity");
        mappings.put("bucket", "?bucket");

        MongoDBScheme scheme = new MongoDBScheme(properties.getDbUrl(),
                properties.getDbPort(),
                properties.getDbName(),
                "page-view",
                "key",
                columns,
                mappings);

        MongoDBTap tap = new MongoDBTap(scheme);

        Api.execute(tap, toMongoDB);
    }

    public void uniquesMongoDB(Subquery uniquesView) {
        Subquery toMongoDB =
                new Subquery("?key", "?value", "?url", "?granularity", "?bucket")
                        .predicate(uniquesView,
                                "?url", "?granularity", "?bucket", "?value")
                        .predicate(new ToUrlBucketedKey(),
                                "?url", "?granularity", "?bucket")
                        .out("?key");

        // List of columns to be fetched from Mongo
        List<String> columns = new ArrayList<String>();
        columns.add("key");
        columns.add("value");
        columns.add("url");
        columns.add("granularity");
        columns.add("bucket");

        // When writing back to mongodb, you may have Cascading output tuple item names
        // a bit different from your Mongodb ColumnFamily definition. Otherwise, you can
        // simply specify both key and value same.
        Map<String, String> mappings = new HashMap<String, String>();
        mappings.put("key", "?key");
        mappings.put("value", "?value");
        mappings.put("url", "?url");
        mappings.put("granularity", "?granularity");
        mappings.put("bucket", "?bucket");

        MongoDBScheme scheme = new MongoDBScheme(properties.getDbUrl(),
                properties.getDbPort(),
                properties.getDbName(),
                "unique-view",
                "key",
                columns,
                mappings);

        MongoDBTap tap = new MongoDBTap(scheme);

        Api.execute(tap, toMongoDB);
    }

    public void bounceRateMongoDB(Subquery bounceView) {
        Subquery toMongoDB =
                new Subquery("?key", "?domain", "?bounces", "?total", "?rate")
                        .predicate(bounceView,
                                "?domain", "?bounces", "?total")
                        .predicate(new ToSerializedString(),
                                "?domain").out("?key")
                        .predicate(new ToBounceRate(), "?bounces", "?total").out("?rate");

        // List of columns to be fetched from Mongo
        List<String> columns = new ArrayList<String>();
        columns.add("key");
        columns.add("domain");
        columns.add("bounces");
        columns.add("total");
        columns.add("rate");

        // When writing back to mongodb, you may have Cascading output tuple item names
        // a bit different from your Mongodb ColumnFamily definition. Otherwise, you can
        // simply specify both key and value same.
        Map<String, String> mappings = new HashMap<String, String>();
        mappings.put("key", "?key");
        mappings.put("domain", "?domain");
        mappings.put("bounces", "?bounces");
        mappings.put("total", "?total");
        mappings.put("rate", "?rate");

        MongoDBScheme scheme = new MongoDBScheme(properties.getDbUrl(),
                properties.getDbPort(),
                properties.getDbName(),
                "bounce-view",
                "key",
                columns,
                mappings);

        MongoDBTap tap = new MongoDBTap(scheme);

        Api.execute(tap, toMongoDB);
    }

    public void pageviewElephantDB(Subquery pageviewBatchView) {
        Subquery toEdb =
                new Subquery("?key", "?value")
                        .predicate(pageviewBatchView,
                                "?url", "?granularity", "?bucket", "?total-pageviews")
                        .predicate(new ToUrlBucketedKey(),
                                "?url", "?granularity", "?bucket")
                        .out("?key")
                        .predicate(new ToSerializedLong(), "?total-pageviews")
                        .out("?value");
        Api.execute(EDB.makeKeyValTap(
                OUTPUTS_ROOT + "edb/pageviews",
                new DomainSpec(new JavaBerkDB(),
                        new UrlOnlyScheme(),
                        32)),
                toEdb);
    }

    public void uniquesElephantDB(Subquery uniquesView) {
        Subquery toEdb =
                new Subquery("?key", "?value")
                        .predicate(uniquesView,
                                "?url", "?granularity", "?bucket", "?value")
                        .predicate(new ToUrlBucketedKey(),
                                "?url", "?granularity", "?bucket")
                        .out("?key");


        Api.execute(EDB.makeKeyValTap(
                OUTPUTS_ROOT + "edb/uniques",
                new DomainSpec(new JavaBerkDB(),
                        new UrlOnlyScheme(),
                        32)),
                toEdb);
    }

    public void bounceRateElephantDB(Subquery bounceView) {
        Subquery toEdb =
            new Subquery("?key", "?value")
                .predicate(bounceView,
                   "?domain", "?bounces", "?total")
                .predicate(new ToSerializedString(),
                    "?domain").out("?key")
                .predicate(new ToSerializedLongPair(),
                    "?bounces", "?total").out("?value");

        Api.execute(EDB.makeKeyValTap(
                        OUTPUTS_ROOT + "edb/bounces",
                        new DomainSpec(new JavaBerkDB(),
                                       new HashModScheme(),
                                       32)),
                    toEdb);
    }

    public Subquery uniquesView() {
        Tap source = splitDataTap("/tmp/swa/unique_pageviews");

        Subquery hourlyRollup =
                new Subquery("?url", "?hour-bucket", "?hyper-log-log")
                    .predicate(source, "_", "?pageview")
                    .predicate(
                        new ExtractPageViewFields(), "?pageview")
                        .out("?url", "?user", "?timestamp")
                    .predicate(new ToHourBucket(), "?timestamp")
                        .out("?hour-bucket")
                    .predicate(new ConstructHyperLogLog(), "?user")
                        .out("?hyper-log-log");

        return new Subquery(
            "?url", "?granularity", "?bucket", "?aggregate-hll")
            .predicate(hourlyRollup,
                       "?url", "?hour-bucket", "?hourly-hll")
            .predicate(new EmitGranularities(), "?hour-bucket")
                .out("?granularity", "?bucket")
            .predicate(new MergeHyperLogLog(), "?hourly-hll")
                .out("?aggregate-hll");
    }

    public Subquery bouncesView() {
        Tap source = splitDataTap("/tmp/swa/unique_pageviews");

        Subquery userVisits =
                new Subquery("?domain", "?user",
                             "?num-user-visits", "?num-user-bounces")
                    .predicate(source, "_", "?pageview")
                    .predicate(
                        new ExtractPageViewFields(), "?pageview")
                        .out("?url", "?user", "?timestamp")
                    .predicate(new ExtractDomain(), "?url")
                        .out("?domain")
                    .predicate(Option.SORT, "?timestamp")
                    .predicate(new AnalyzeVisits(), "?timestamp")
                        .out("?num-user-visits", "?num-user-bounces");

        return new Subquery("?domain", "?num-visits", "?num-bounces")
            .predicate(userVisits, "?domain", "_",
                       "?num-user-visits", "?num-user-bounces")
            .predicate(new Sum(), "?num-user-visits")
                .out("?num-visits")
            .predicate(new Sum(), "?num-user-bounces")
                .out("?num-bounces");
    }

    public Tap runUserIdNormalizationIteration(int i) {
        Object source = Api.hfsSeqfile(
                properties.getNamenodeUrl() + "/tmp/swa/equivs" + (i - 1));
        Object sink = Api.hfsSeqfile(properties.getNamenodeUrl() + "/tmp/swa/equivs" + i);

        Object iteration = new Subquery(
                "?b1", "?node1", "?node2", "?is-new")
                .predicate(source, "?n1", "?n2")
                .predicate(new BidirectionalEdge(), "?n1", "?n2")
                    .out("?b1", "?b2")
                .predicate(new IterateEdges(), "?b2")
                    .out("?node1", "?node2", "?is-new");

        iteration = Api.selectFields(iteration,
                new Fields("?node1", "?node2", "?is-new"));

        Subquery newEdgeSet = new Subquery("?node1", "?node2")
                .predicate(iteration, "?node1", "?node2", "?is-new")
                .predicate(Option.DISTINCT, true);

        Tap progressEdgesSink = new Hfs(new SequenceFile(cascading.tuple.Fields.ALL),
                properties.getNamenodeUrl() + "/tmp/swa/equivs" + i + "-new");
        Subquery progressEdges = new Subquery("?node1", "?node2")
                .predicate(iteration, "?node1", "?node2", true);

        Api.execute(Arrays.asList((Object)sink, progressEdgesSink),
                    Arrays.asList((Object)newEdgeSet, progressEdges));
        return progressEdgesSink;
    }

    public void normalizeUserIds() throws IOException {
        Tap equivs = attributeTap("/tmp/swa/normalized_urls",
                                  DataUnit._Fields.EQUIV);
        Api.execute(Api.hfsSeqfile(properties.getNamenodeUrl() + "/tmp/swa/equivs0"),
                new Subquery("?node1", "?node2")
                    .predicate(equivs, "_", "?data")
                    .predicate(new EdgifyEquiv(), "?data")
                      .out("?node1", "?node2"));
        int i = 1;
        while(true) {
          Tap progressEdgesSink = runUserIdNormalizationIteration(i);

          if(!new HadoopFlowProcess(new JobConf())
                  .openTapForRead(progressEdgesSink)
                  .hasNext()) {
              break;
          }
          i++;
        }

        Tap pageviews = attributeTap("/tmp/swa/normalized_urls",
                DataUnit._Fields.PAGE_VIEW);
        Object newIds = Api.hfsSeqfile(properties.getNamenodeUrl() + "/tmp/swa/equivs" + i);
        Tap result = splitDataTap(
                        "/tmp/swa/normalized_pageview_users");

        Api.execute(result,
                new Subquery("?normalized-pageview")
                    .predicate(newIds, "!!newId", "?person")
                    .predicate(pageviews, "_", "?data")
                    .predicate(new ExtractPageViewFields(), "?data")
                              .out("?userid", "?person", "?timestamp")
                    .predicate(new MakeNormalizedPageview(),
                        "!!newId", "?data").out("?normalized-pageview"));
    }

    public void run() throws Exception {
        init();
        setApplicationConf();
        initTestData();

        Pail masterPail = pailService.getPail(MASTER_ROOT);
        Pail newDataPail = pailService.getPail(NEW_ROOT);

        ingest(masterPail, newDataPail);
        normalizeURLs();
        normalizeUserIds();
        deduplicatePageviews();

        pageviewMongoDB(pageviewBatchView());
        uniquesMongoDB(uniquesView());
        bounceRateMongoDB(bouncesView());
    }
}
