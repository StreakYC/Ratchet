package com.streak.ratchet;

import com.google.cloud.spanner.*;
import com.google.cloud.spanner.testing.RemoteSpannerHelper;
import com.streak.ratchet.entities.Complex;
import com.streak.ratchet.entities.DoubleNested;
import com.streak.ratchet.entities.NestedList;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class IntegrationTests {
    private static final long ID_OFFSET = System.currentTimeMillis();
    private static final String INSTANCE_ID = "spannerdev";
    private static RemoteSpannerHelper helper;
    private static Database testDatabase;
    private static ExecutorService workerPool;

    @BeforeClass
    public static void setUp() throws Throwable {
        SpannerOptions options = SpannerOptions.newBuilder().build();
        helper = RemoteSpannerHelper.create(InstanceId.of(options.getProjectId(), INSTANCE_ID));
        Configuration.INSTANCE.register(Complex.class);
        Configuration.INSTANCE.register(NestedList.class);
        Configuration.INSTANCE.register(DoubleNested.class);
        List<String> ddl = new ArrayList<>();
        ddl.addAll(Configuration.INSTANCE.getMetadata(Complex.class).getTable().ddl());
        ddl.addAll(Configuration.INSTANCE.getMetadata(NestedList.class).getTable().ddl());
        ddl.addAll(Configuration.INSTANCE.getMetadata(DoubleNested.class).getTable().ddl());
        testDatabase = helper.createTestDatabase(ddl);
        Configuration.INSTANCE.setSpannerProvider(() -> helper.getDatabaseClient(testDatabase));
        workerPool = Executors.newFixedThreadPool(2);
    }

    @AfterClass
    public static void tearDown() {
        helper.cleanUp();
        workerPool.shutdown();
    }


    @Test
    public void complexSave() {
        Complex complex = new Complex();

        complex.id = ID_OFFSET + 1;
        complex.version = 1L;
        complex.sub = new ArrayList<>();
        complex.sub.add(new Complex.Subclass(Complex.STATUS.GOOD));
        complex.sub.add(new Complex.Subclass(Complex.STATUS.BAD));

        new RatchetReaderWriter().save(complex);

        Complex fromSpanner = new RatchetReaderWriter().load(
                Complex.class,
                complex.id);

        assert fromSpanner != null;
        Assert.assertEquals(2, fromSpanner.sub.size());
        Assert.assertEquals(Complex.STATUS.GOOD, fromSpanner.sub.get(0).status);
        Assert.assertEquals(Complex.STATUS.BAD, fromSpanner.sub.get(1).status);
    }

    @Test
    public void complexTest() throws Throwable {
        Complex complex = new Complex();

        complex.id = ID_OFFSET + 2;
        complex.version = 1L;
        complex.sub = new ArrayList<>();
        complex.sub.add(new Complex.Subclass(Complex.STATUS.GOOD));
        complex.sub.add(new Complex.Subclass(Complex.STATUS.BAD));
        complex.primitive = 42;

        MutationFactory mutationFactory = new MutationFactory(complex);

        List<Mutation> mutations = mutationFactory.createInsert();

        Assert.assertEquals(mutations.size(), 3);
        Map<String, Value> goodMutation = mutations.get(1).asMap();
        assert goodMutation.containsKey("sub_status");

        new RatchetReaderWriter().save(complex);

        Complex fromSpanner = new RatchetReaderWriter().load(
                Complex.class,
                complex.id);

        Assert.assertEquals(2, fromSpanner.sub.size());
        Assert.assertEquals(42, fromSpanner.primitive);

        List<Complex> allVersions = new RatchetReaderWriter().loadAll(
                Complex.class,
                complex.id);

        Assert.assertEquals(1, allVersions.size());
    }

    @Test
    public void testDoubleList() {


        DoubleNested dn = new DoubleNested();
        dn.id = ID_OFFSET + 95L;
        dn.sub = new ArrayList<>();
        dn.sub.add(new ArrayList<>());
        dn.sub.get(0).add(5L);
        dn.sub.get(0).add(3L);

        new RatchetReaderWriter().save(dn);

        DoubleNested fromSpanner = new RatchetReaderWriter().load(DoubleNested.class, ID_OFFSET + 95L);
        Assert.assertEquals(dn, fromSpanner);
    }

    @Test
    public void testNestedList() {


        NestedList nl = new NestedList();
        nl.id = ID_OFFSET;
        nl.bool = true;
        nl.sc = new NestedList.SubContainer();
        nl.sc.onParent = 4;
        nl.sc.longs = new ArrayList<>();
        nl.sc.longs.add(3L);
        nl.sc.longs.add(5L);
        nl.sc.longs.add(8L);
        nl.sc.subData = new ArrayList<>();
        nl.sc.subData.add(new NestedList.SubSimple(2L, 96L));
        nl.sc.subData.add(new NestedList.SubSimple(78L, 442L));

        new RatchetReaderWriter().save(nl);
        NestedList fromSpanner = new RatchetReaderWriter().load(NestedList.class, ID_OFFSET);
        Assert.assertEquals(nl, fromSpanner);
    }

    @Test
    public void basic() {
        Complex complex = new Complex();
        complex.id = ID_OFFSET + 50;
        complex.sub = new ArrayList<>();
        new RatchetReaderWriter().save(complex);
        Assert.assertEquals((long) complex.version, 1L);
        complex.primitive = 2;
        new RatchetReaderWriter().save(complex);
        Assert.assertEquals(2L, (long) complex.version);
        Complex loadedComplex = new RatchetReaderWriter().load(Complex.class, complex.id);
        Assert.assertNotNull(loadedComplex);
        Assert.assertEquals(2, (long) loadedComplex.version);

        new RatchetReaderWriter().delete(complex);
        Assert.assertNull(new RatchetReaderWriter().load(Complex.class, complex.id));

        List<Complex> allVersions = new RatchetReaderWriter().loadAll(Complex.class, complex.id);
        Assert.assertEquals(allVersions.size(), 2);
    }

    @Test
    public void readOnlyTransaction() {
        Complex c1 = new Complex();
        c1.id = ID_OFFSET + 34;
        c1.primitive = 1000;
        new RatchetReaderWriter().save(c1);
        Complex c2 = new Complex();
        c2.id = ID_OFFSET + 35;
        c2.primitive = 1001;
        new RatchetReaderWriter().save(c2);

        try (RatchetReadOnlyTransaction txn = new RatchetReadOnlyTransaction()) {

            // make a read within the transaction to start it;
            Complex loadedC1 = txn.load(Complex.class, c1.id);
            assert loadedC1 != null;
            Assert.assertEquals(1000, loadedC1.primitive);

            // Write to c2 outside of the read-only txn;
            c2.primitive = 1002;
            new RatchetReaderWriter().save(c2);

            // Check that save worked by loading c2 outside of transaction;
            Complex loadedC2 = new RatchetReaderWriter().load(Complex.class, c2.id);
            assert loadedC2 != null;
            Assert.assertEquals(1002, loadedC2.primitive);

            // Check that c2 has old value when read inside of the readonly transaction;
            Complex loadedC2txn = txn.load(Complex.class, c2.id);
            assert loadedC2txn != null;
            Assert.assertEquals(1001, loadedC2txn.primitive);
        }
    }

    @Test
    public void readWriteTransaction() throws ExecutionException, InterruptedException {
        Complex c1 = new Complex();

        c1.id = ID_OFFSET + 989;

        c1.primitive = 1000;
        new RatchetReaderWriter().save(c1);

        List<Future<?>> jobs = new ArrayList<>();

        ReadWriteTransaction<String> ret = new ReadWriteTransaction<>((RatchetReaderWriter txn) -> {
            // Make a read in the txn to anchor it;
            Complex loadedC1 = txn.load(Complex.class, c1.id);
            assert loadedC1 != null;
            Assert.assertEquals(1000, loadedC1.primitive);
            ;
            // While this transaction is going, start a new thread that tries to write to c1.;
            // This read-write transaction should lock c1, causing the new thread to wait on it / retry;
            // and then see this transaction's results.;
            jobs.add(workerPool.submit(() -> {
                ReadWriteTransaction<Long> wrotePrimitive = new ReadWriteTransaction<>((RatchetReaderWriter secondTxn) -> {
                    Complex c1ForOutsideWrite = secondTxn.load(Complex.class, c1.id);
                    assert c1ForOutsideWrite != null;
                    c1ForOutsideWrite.primitive += 1;
                    secondTxn.save(c1ForOutsideWrite);
                    return c1ForOutsideWrite.primitive;
                });
                wrotePrimitive.run();
                Assert.assertEquals(2001, (long) wrotePrimitive.getResult());
            }));

            loadedC1.primitive = 2000;
            txn.save(loadedC1);
            ;
            // The write shouldn't be visible even within the transaction until it commits;
            Complex secondLoadedC1 = txn.load(Complex.class, c1.id);
            assert secondLoadedC1 != null;
            Assert.assertEquals(1000, secondLoadedC1.primitive);
            return "done";
        });
        ret.run();
        Assert.assertEquals("done", ret.getResult());
        Assert.assertNotNull(ret.getCommitTimestamp());

        // Wait on the extra writer thread to end;
        for (Future<?> it : jobs) {
            it.get();
        }

        Complex c1AtEnd = new RatchetReaderWriter().load(Complex.class, c1.id);
        assert c1AtEnd != null;
        Assert.assertEquals(2001, c1AtEnd.primitive);
    }
}
