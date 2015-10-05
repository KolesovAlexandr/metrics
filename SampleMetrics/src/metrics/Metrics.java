package metrics;

import util.BatchBuffer;
import util.BatchConsumer;

import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Metrics {

    private static final int QUEUE_CAPACITY = 2;
    public static final int MAX_PAIRS = 500;
    public static final int CORE_POOL_SIZE = 2;
    public static final int MAXIMUM_POOL_SIZE = 10;
    public static final int CAPACITY = 1;

    public static void main(final String[] args) {

        System.out.println("Start");

        long start = System.nanoTime();

        Metrics metrics = new Metrics();

        metrics.doWork();

        long stop = System.nanoTime();
        System.out.println("Finish");
        System.out.println("Elapsed: " + (stop - start));
    }

    private static final class FullyBlockingQueue<T> extends LinkedBlockingQueue<T> {

        private FullyBlockingQueue(int capacity) {
            super(capacity);
        }

        @Override
        public boolean offer(T e) {
            try {
                put(e);
                return true;
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private void doWork() {

        Data data = new Data();
        ReferenceSequence sequence = new ReferenceSequence();

        final AtomicLong total = new AtomicLong(0);

        // Iterator<Record> it = data.iterator();
        //
        // while (it.hasNext()) {
        // Record record = it.next();
        //
        // ///
        // }


        ExecutorService tt = Executors.newCachedThreadPool();
        final ThreadPoolExecutor service_prcessing = new ThreadPoolExecutor(CORE_POOL_SIZE, MAXIMUM_POOL_SIZE, 0L, TimeUnit.MILLISECONDS, new FullyBlockingQueue<>(CAPACITY));
        final ThreadPoolExecutor service_reader = new ThreadPoolExecutor(CORE_POOL_SIZE, MAXIMUM_POOL_SIZE, 0L, TimeUnit.MILLISECONDS, new FullyBlockingQueue<>(CAPACITY));
        BatchBuffer<Object[]> batcher = new BatchBuffer<>(MAX_PAIRS, new BatchConsumer<Object[]>() {
            @Override
            public void consume(Collection<Object[]> batch) {
                service_prcessing.execute(new Sum(batch, total));
            }
        });


        for (final Record record : data) {


            final Reference ref = sequence.getRef(record);
//			try {
//				Thread.sleep(1000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
            batcher.add(new Object[]{record, ref});

//

        }
        batcher.flush();

    /*    try {
            queue.put(poisonPill);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/

        service_prcessing.shutdown();

        try {
            service_prcessing.awaitTermination(1, TimeUnit.DAYS);


        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(total.get());

    }

    private int process(final Record record, final Reference ref) {
        int sum = 0;
        for (int i = 0; i < record.read.length; i++) {
            sum += record.read[i];
            sum += ref.read[i];
        }
        return sum;
    }

    private class Sum implements Runnable {
        private final Collection<Object[]> batch;
        private final AtomicLong total;

        public Sum(Collection<Object[]> batch, AtomicLong total) {
            this.batch = batch;
            this.total = total;
        }

        @Override
        public void run() {

            int sum = 0;

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            for (Object[] objects : batch) {
                Record rec = (Record) objects[0];
                Reference ref = (Reference) objects[1];
                sum += process(rec, ref);
            }

            total.addAndGet(sum); // CAS compare and swap


        }
    }
}
