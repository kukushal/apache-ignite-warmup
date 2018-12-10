package apache.ignite.warmup;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

import javax.cache.Cache;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class App {
    public static void main(String[] args) {
        final String CACHE_NAME = "cache1";

        try (Ignite ignored = Ignition.start("ignite-server.xml");
             Ignite ignite = Ignition.start("ignite-client.xml")) {
            ignite.cluster().active(true);

            if (args.length == 0) {
                load(ignite, CACHE_NAME);
                System.out.println(">>> Data was persisted.");
            }
            else if (args[0].toUpperCase().equals("PUBLIC")) {
                warmup(ignite, CACHE_NAME, true);
                System.out.println(">>> Data was warmed up using public Ignite API.");
            }
            else if (args[0].toUpperCase().equals("INTERNAL")) {
                warmup(ignite, CACHE_NAME, false);
                System.out.println(">>> Data was warmed up using internal Ignite API.");
            }
            else
                System.out.println("Unsupported command: " + args[0]);
        }
    }

    private static void load(Ignite ignite, String cacheName) {
        Cache<Integer, String> cache = ignite.getOrCreateCache(
            new CacheConfiguration<Integer, String>(cacheName).setBackups(1)
        );

        cache.putAll(
            IntStream.range(0, 10_000)
                .mapToObj(i -> new SimpleEntry<>(i, Integer.toString(i)))
                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue))
        );
    }

    /**
     * Preload cache data into Ignite off-heap memory.
     * @param shallUsePublicApi Indicates whether to use public but slow API or fast but internal API tested only
     * with Ignite 2.6.
     */
    public static <K> void warmup(Ignite ignite, String cacheName, boolean shallUsePublicApi) {
        Affinity<K> aff = ignite.affinity(cacheName);

        for (int p = 0; p < aff.partitions(); p++)
            ignite.compute().affinityRun(
                Collections.singleton(cacheName),
                p,
                shallUsePublicApi ? new PublicWarmupJob(cacheName, p) : new InternalWarmupJob(cacheName, p));
    }

    /**
     * Warm-up job using public Ignite API. This job is slower than {@link InternalWarmupJob} since it loads
     * each record into on-heap memory including serialization. The advantage is that the public API is reliable and
     * should work with every Ignite version.
     */
    private static class PublicWarmupJob implements IgniteRunnable {
        @IgniteInstanceResource private Ignite ignite;

        private final String cacheName;
        private final int part;

        public PublicWarmupJob(String cacheName, int part) {
            this.cacheName = cacheName;
            this.part = part;
        }

        @Override public void run() {
            try (QueryCursor cur = ignite.cache(cacheName).query(new ScanQuery().setLocal(true).setPartition(part))) {
                for (Object ignored : cur);
            }
        }
    }

    /**
     * Warm-up job using private Ignite 2.6 API. This job is faster than {@link PublicWarmupJob} since loads data into
     * off-heap memory by pages without any on-heap and entry-level overhead. The drawback is the private Ignite API
     * might not be available in any other release.
     */
    private static class InternalWarmupJob implements IgniteRunnable {
        @IgniteInstanceResource private Ignite ignite;

        private final String cacheName;
        private final int part;

        public InternalWarmupJob(String cacheName, int part) {
            this.cacheName = cacheName;
            this.part = part;
        }

        @Override public void run() {
            try {
                IgniteInternalCache cache = ((IgniteKernal)ignite).context().cache().cache(cacheName);
                IgnitePageStoreManager pageStoreMgr = cache.context().shared().pageStore();
                CacheGroupContext grp = cache.context().group();
                PageMemoryEx pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();

                if (pageStoreMgr == null)
                    return;

                final int pages = pageStoreMgr.pages(grp.groupId(), part);

                long pageId = pageMem.partitionMetaPageId(grp.groupId(), part);

                // For each page sequentially pin/unpin.
                for (int pageNo = 0; pageNo < pages; pageId++, pageNo++) {
                    long pagePointer = -1;

                    try {
                        pagePointer = pageMem.acquirePage(grp.groupId(), pageId);
                    }
                    finally {
                        if (pagePointer != -1)
                            pageMem.releasePage(grp.groupId(), pageId, pagePointer);
                    }
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
