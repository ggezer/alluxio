/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.metastore.rocks;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.InodeStore.WriteBatch;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Iterator;

public class RocksInodeStoreTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Test
  public void batchWrite() throws IOException {
    RocksInodeStore store = new RocksInodeStore(mFolder.newFolder().getAbsolutePath());
    WriteBatch batch = store.createWriteBatch();
    for (int i = 1; i < 20; i++) {
      batch.writeInode(
          MutableInodeDirectory.create(i, 0, "dir" + i, CreateDirectoryContext.defaults()));
    }
    batch.commit();
    for (int i = 1; i < 20; i++) {
      assertEquals("dir" + i, store.get(i).get().getName());
    }
  }

  @Test
  public void toStringEntries() throws IOException {
    RocksInodeStore store = new RocksInodeStore(mFolder.newFolder().getAbsolutePath());
    assertEquals("", store.toStringEntries());

    store.writeInode(MutableInodeDirectory.create(1, 0, "dir", CreateDirectoryContext.defaults()));
    assertEquals("dir", store.get(1).get().getName());
    assertThat(store.toStringEntries(), containsString("name=dir"));
  }

  @Test
  public void indices() throws IOException {
    RocksInodeStore store = new RocksInodeStore(mFolder.newFolder().getAbsolutePath());
    InodeStore.InodeIndice indice = store.getIndice(InodeStore.InodeIndiceType.REPLICATION_LIMITED);
    final int indiceCount = 10000000;
    long queryStart = System.currentTimeMillis();
    for (long i = 0; i < indiceCount; i++) {
      indice.set(i);
    }
    long queryEnd = System.currentTimeMillis();
    System.out.println(
        String.format("Adding %s entries took %s ms.", indiceCount, queryEnd - queryStart));

    queryStart = System.currentTimeMillis();
    assertEquals(indiceCount, indice.size());
    queryEnd = System.currentTimeMillis();
    System.out.println(
        String.format("Size() for %s entries took %s ms.", indiceCount, queryEnd - queryStart));

    Iterator<Long> indiceIter = indice.iterator();

    int iterated = 0;
    queryStart = System.currentTimeMillis();
    while (indiceIter.hasNext()) {
      indiceIter.next();
      iterated++;
    }
    assertEquals(iterated, indiceCount);
    queryEnd = System.currentTimeMillis();
    System.out.println(
        String.format("Enumerating %s entries took %s ms.", indiceCount, queryEnd - queryStart));
  }
}
