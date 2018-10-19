package com.facebook.presto.raptorx.storage;

import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindList;

import java.util.List;

public class ChunkAccessRecorder
{
    public void accessedChunk(long chunkId)
    {
        //TODO every few minutes, collect read chunk and write the last access time
    }

    interface ChuckAccessDao {
        void updateAccessTime(
                @BindList("chucks")List<Long> chucks,
                @Bind("time") long timestamp);
    }
}
