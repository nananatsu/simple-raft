
本项目实现了一个简单的分布式kv数据库，共识算法使用Raft，下层存储使用lsm，节点间使用grpc进行通信。

已实现功能：
1. [领导选取](./doc/raft_learn_1.md)
2. [日志同步](./doc/raft_learn_2.md)
3. 日志压缩
4. 成员变更

## 参考

- [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [CONSENSUS: BRIDGING THEORY AND PRACTICE](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
- [etcd/raft](https://github.com/etcd-io/etcd)
- [leveldb](https://github.com/google/leveldb)
- [goleveldb](https://github.com/syndtr/goleveldb)



    

