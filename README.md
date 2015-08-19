BaseX over LMDB

After BaseX Core stripped to its bare essentials MemData and TableMemAccess will be extended to work on top of [LMDB](http://symas.com/mdb/) with [lmdbjni](https://github.com/deephacks/lmdbjni).

This is a POC.

The idea is to strengthen the BaseX store structure and later replicate it for further high availability.
