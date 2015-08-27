BaseX over LMDB

After BaseX Core stripped to its bare essentials a LMDBData snd TableLMDBAccess will be created to work on top of [LMDB](http://symas.com/mdb/) with [lmdbjni](https://github.com/deephacks/lmdbjni).

This is a POC.

The idea is to strengthen the BaseX store structure and later replicate it (with [BookKeeper](http://bookkeeper.apache.org/)?, [jgropus-raft](https://github.com/belaban/jgroups-raft/blob/master/doc/manual/overview.adoc)?) for further high availability.
