package storage

import (
	"time"
	"context"
	"errors"
	"testing"

	"go.etcd.io/etcd/client/v3"
	"github.com/stretchr/testify/assert"
	"github.com/KvrocksLabs/kvrocks-controller/storage/base/etcd"
	"github.com/KvrocksLabs/kvrocks-controller/storage/base/memory"
	"github.com/KvrocksLabs/kvrocks-controller/metadata"
)

func GetCluster() *metadata.Cluster{	
	return &metadata.Cluster{
		Version: 1,
		Shards: []metadata.Shard{
			metadata.Shard{
				Nodes:[]metadata.NodeInfo {
					metadata.NodeInfo{
						ID: 		"2bcefa7dff0aed57cacbce90134434587a10c891",
						CreatedAt:  time.Now().Unix(),
						Address: 	"127.0.0.1:6121",
						Role: 		metadata.RoleMaster,
						RequirePassword: "password",
						MasterAuth: 	 "auth",
					},
					metadata.NodeInfo{
						ID: 		"75d76824d2e903af52b8c356941908132fef6b9f",
						CreatedAt:  time.Now().Unix(),
						Address: 	"127.0.0.1:6122",
						Role: 		metadata.RoleSlave,
						RequirePassword: "password",
						MasterAuth: 	 "auth",
					},
				},
				SlotRanges:[]metadata.SlotRange{
					metadata.SlotRange{
						Start: 0,
						Stop:  4095,
					},
					metadata.SlotRange{
						Start: 8192,
						Stop:  16383,
					},
				},
				ImportSlot:   	4096,        
				MigratingSlot: 	8192, 
			},
			metadata.Shard{
				Nodes:[]metadata.NodeInfo {
					metadata.NodeInfo{
						ID: 		"415cb13e439236d0fec257883e8ae1eacaa42244",
						CreatedAt:  time.Now().Unix(),
						Address: 	"127.0.0.1:6123",
						Role: 		metadata.RoleMaster,
						RequirePassword: "password",
						MasterAuth: 	 "auth",
					},
				},
				SlotRanges:[]metadata.SlotRange {
					metadata.SlotRange{
						Start: 4096,
						Stop:  8191,
					},
				},
				ImportSlot:   	8192,        
				MigratingSlot: 	4096, 
			},
		},
		Config: metadata.ClusterConfig {
			Name: 				"test_cluster",
  			HeartBeatInterval: 	1,
  			HeartBeatRetrys:	2,
		},
	}
}

func GetStorage(id string) (*Storage, error) {
	return NewStorage(id, []string{"127.0.0.1:2379"})
}

// Election unittest just designed to the basic scenario,
// "go.etcd.io/etcd/client/v3/concurrency", use leases to
// implement elections , leases delay attribute which is 
// inconvenient for unittest of leader-follower switching
func TestStorage_Election(t *testing.T) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	_, err = client.Delete(context.TODO(), etcd.LeaderKey, clientv3.WithPrefix())
	assert.Equal(t, nil, err)

	stor1, _:= GetStorage("127.0.0.1:9134")
	stor2, _:= GetStorage("127.0.0.1:9135")
	assert.Equal(t, true, stor1.SelfLeader())
	assert.Equal(t, false, stor2.SelfLeader())
	select {
	case res := <- stor1.BecomeLeader():
		assert.Equal(t, true, res)
	case res := <- stor2.BecomeLeader():
		assert.Equal(t, false, res)
	}
}

func TestStorage_Namespace(t *testing.T) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	_, err = client.Delete(context.TODO(), etcd.LeaderKey, clientv3.WithPrefix())
	assert.Equal(t, nil, err)

	stor1, _ := GetStorage("127.0.0.1:9134")
	err = stor1.CreateNamespace("testNs")
	assert.Equal(t, nil, err)
	err = stor1.CreateNamespace("testNs")
	assert.Equal(t, metadata.ErrNamespaceHasExisted, err)
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, EventNamespace, e.Type)
		assert.Equal(t, Command(CommandCreate), e.Command)
	}

	has, err := stor1.HasNamespace("testNs")
	assert.Equal(t, nil, err)
	assert.Equal(t, true, has)
	has, err = stor1.HasNamespace("testNsCopy")
	assert.Equal(t, nil, err)
	assert.Equal(t, false, has)

	ns, err := stor1.ListNamespace()
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(ns))

	stor1.CreateCluster("testNs", "testCluster", GetCluster())
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, "testCluster", e.Cluster)
		assert.Equal(t, EventCluster, e.Type)
		assert.Equal(t, Command(CommandCreate), e.Command)
	}
	err = stor1.RemoveNamespace("testNs")
	assert.Equal(t, errors.New("namespace wasn't empty, please remove clusters first"), err)
	stor1.RemoveCluster("testNs", "testCluster")
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, "testCluster", e.Cluster)
		assert.Equal(t, EventCluster, e.Type)
		assert.Equal(t, Command(CommandRemove), e.Command)
	}
	err = stor1.RemoveNamespace("testNs")
	assert.Equal(t, nil, err)
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, EventNamespace, e.Type)
		assert.Equal(t, Command(CommandRemove), e.Command)
	}
	err = stor1.RemoveNamespace("testNsCopy")
	assert.Equal(t, metadata.ErrNamespaceNoExists, err)
}

func TestStorage_LoadCluster(t *testing.T) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	_, err = client.Delete(context.TODO(), etcd.LeaderKey, clientv3.WithPrefix())
	assert.Equal(t, nil, err)

	stor1, _ := GetStorage("127.0.0.1:9134")
	err = stor1.CreateNamespace("testNs")
	stor1.CreateCluster("testNs", "testCluster", GetCluster())
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, EventNamespace, e.Type)
		assert.Equal(t, Command(CommandCreate), e.Command)
	}
	stor1.CreateCluster("testNs", "testCluster", GetCluster())
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, "testCluster", e.Cluster)
		assert.Equal(t, EventCluster, e.Type)
		assert.Equal(t, Command(CommandCreate), e.Command)
	}

	err = stor1.CreateNamespace("testNsCopy")
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNsCopy", e.Namespace)
		assert.Equal(t, EventNamespace, e.Type)
		assert.Equal(t, Command(CommandCreate), e.Command)
	}
	stor1.CreateCluster("testNsCopy", "testClusterCopy", GetCluster())
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNsCopy", e.Namespace)
		assert.Equal(t, "testClusterCopy", e.Cluster)
		assert.Equal(t, EventCluster, e.Type)
		assert.Equal(t, Command(CommandCreate), e.Command)
	}

	stor1.local = memory.NewMemStorage()
	err = stor1.LoadCluster()
	assert.Equal(t, nil, err)
	namespcaes , err := stor1.ListNamespace()
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(namespcaes))
	assert.Equal(t, "testNs", namespcaes[0])
	assert.Equal(t, "testNsCopy", namespcaes[1])
	has, err := stor1.HasCluster("testNs", "testCluster")
	assert.Equal(t, nil, err)
	assert.Equal(t, true, has)
	has, err = stor1.HasCluster("testNsCopy", "testClusterCopy")
	assert.Equal(t, nil, err)
	assert.Equal(t, true, has)
}

func TestStorage_Cluster(t *testing.T) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	_, err = client.Delete(context.TODO(), etcd.LeaderKey, clientv3.WithPrefix())
	assert.Equal(t, nil, err)

	stor1, _ := GetStorage("127.0.0.1:9134")
	stor1.CreateNamespace("testNs")
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, EventNamespace, e.Type)
		assert.Equal(t, Command(CommandCreate), e.Command)
	}
	cluster := GetCluster()
	err = stor1.CreateCluster("testNs", "testCluster", cluster)
	assert.Equal(t, nil, err)
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, "testCluster", e.Cluster)
		assert.Equal(t, EventCluster, e.Type)
		assert.Equal(t, Command(CommandCreate), e.Command)
	}
	err = stor1.CreateCluster("testNs", "testCluster", cluster)
	assert.Equal(t, metadata.ErrClusterHasExisted, err)
	err = stor1.CreateCluster("testNsCopy", "testCluster", cluster)
	assert.Equal(t, metadata.ErrNamespaceNoExists, err)
	err = stor1.CreateCluster("testNs", "testClusterCopy", &metadata.Cluster{})
	assert.Equal(t, errors.New("required at least one shard"), err)
	// read etcd
	remoteCluster, err := stor1.remote.GetClusterCopy("testNs", "testCluster")
	assert.Equal(t, nil, err)
	assert.Equal(t, "127.0.0.1:6121", remoteCluster.Shards[0].Nodes[0].Address)

	cluster.Shards[0].Nodes[0].Address = "127.0.0.1:6379"
	err = stor1.UpdateCluster("testNs", "testCluster", cluster)
	assert.Equal(t, nil, err)
	clusterCopy, err := stor1.GetClusterCopy("testNs", "testCluster")
	assert.Equal(t, nil, err)
	assert.Equal(t, "127.0.0.1:6379", clusterCopy.Shards[0].Nodes[0].Address)
	// read etcd
	remoteClusterCopy, err := stor1.remote.GetClusterCopy("testNs", "testCluster")
	assert.Equal(t, nil, err)
	assert.Equal(t, "127.0.0.1:6379", remoteClusterCopy.Shards[0].Nodes[0].Address)

	err = stor1.RemoveCluster("testNs", "testCluster")
	assert.Equal(t, nil, err)
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, "testCluster", e.Cluster)
		assert.Equal(t, EventCluster, e.Type)
		assert.Equal(t, Command(CommandRemove), e.Command)
	}
	stor1.RemoveNamespace("testNs")
}

func TestStorage_Shard(t *testing.T) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	_, err = client.Delete(context.TODO(), etcd.LeaderKey, clientv3.WithPrefix())
	assert.Equal(t, nil, err)

	stor1, _ := GetStorage("127.0.0.1:9134")
	stor1.CreateNamespace("testNs")
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, EventNamespace, e.Type)
		assert.Equal(t, Command(CommandCreate), e.Command)
	}
	cluster := GetCluster()
	err = stor1.CreateCluster("testNs", "testCluster", cluster)
	assert.Equal(t, nil, err)
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, "testCluster", e.Cluster)
		assert.Equal(t, EventCluster, e.Type)
		assert.Equal(t, Command(CommandCreate), e.Command)
	}

	shard := &metadata.Shard{}
	err = stor1.CreateShard("testNs", "testCluster", shard)
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, "testCluster", e.Cluster)
		assert.Equal(t, 2, e.Shard)
		assert.Equal(t, EventShard, e.Type)
		assert.Equal(t, Command(CommandCreate), e.Command)
	}
	remoteClusterCopy, err := stor1.remote.GetClusterCopy("testNs", "testCluster")
	assert.Equal(t, nil, err)
	assert.Equal(t, 3, len(remoteClusterCopy.Shards))

	err = stor1.RemoveShard("testNs", "testCluster", 2)
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, "testCluster", e.Cluster)
		assert.Equal(t, 2, e.Shard)
		assert.Equal(t, EventShard, e.Type)
		assert.Equal(t, Command(CommandRemove), e.Command)
	}
	remoteClusterCopy, err = stor1.remote.GetClusterCopy("testNs", "testCluster")
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(remoteClusterCopy.Shards))

	slotRanges := []metadata.SlotRange{
		metadata.SlotRange{Start: 0, Stop:  4095},
	}
	err = stor1.RemoveShardSlots("testNs", "testCluster", 0, slotRanges)
	assert.Equal(t, nil, err)
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, "testCluster", e.Cluster)
		assert.Equal(t, 0, e.Shard)
		assert.Equal(t, EventShard, e.Type)
		assert.Equal(t, Command(CommandRemoveSlots), e.Command)
	}
	remoteClusterCopy, err = stor1.remote.GetClusterCopy("testNs", "testCluster")
	assert.Equal(t, nil, err)
	assert.Equal(t, 8192, remoteClusterCopy.Shards[0].SlotRanges[0].Start)
	assert.Equal(t, 16383, remoteClusterCopy.Shards[0].SlotRanges[0].Stop)

	err = stor1.AddShardSlots("testNs", "testCluster", 0, slotRanges)
	assert.Equal(t, nil, err)
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, "testCluster", e.Cluster)
		assert.Equal(t, 0, e.Shard)
		assert.Equal(t, EventShard, e.Type)
		assert.Equal(t, Command(CommandAddSlots), e.Command)
	}
	remoteClusterCopy, err = stor1.remote.GetClusterCopy("testNs", "testCluster")
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, remoteClusterCopy.Shards[0].SlotRanges[0].Start)
	assert.Equal(t, 4095, remoteClusterCopy.Shards[0].SlotRanges[0].Stop)

	err = stor1.RemoveCluster("testNs", "testCluster")
	assert.Equal(t, nil, err)
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, "testCluster", e.Cluster)
		assert.Equal(t, EventCluster, e.Type)
		assert.Equal(t, Command(CommandRemove), e.Command)
	}
	stor1.RemoveNamespace("testNs")
}

func TestStorage_Node(t *testing.T) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	_, err = client.Delete(context.TODO(), etcd.LeaderKey, clientv3.WithPrefix())
	assert.Equal(t, nil, err)

	stor1, _ := GetStorage("127.0.0.1:9134")
	stor1.CreateNamespace("testNs")
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, EventNamespace, e.Type)
		assert.Equal(t, Command(CommandCreate), e.Command)
	}
	cluster := GetCluster()
	err = stor1.CreateCluster("testNs", "testCluster", cluster)
	assert.Equal(t, nil, err)
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, "testCluster", e.Cluster)
		assert.Equal(t, EventCluster, e.Type)
		assert.Equal(t, Command(CommandCreate), e.Command)
	}

	node := &metadata.NodeInfo{
		ID: 		"2bcefa7dff0aed57cacbce90134434587a10c891",
		CreatedAt:  time.Now().Unix(),
		Address: 	"127.0.0.1:6379",
		Role: 		metadata.RoleSlave,
		RequirePassword: "password",
		MasterAuth: 	 "auth",
	}
	err = stor1.UpdateNode("testNs", "testCluster", 0, node)
	assert.Equal(t, nil, err)
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, "testCluster", e.Cluster)
		assert.Equal(t, 0, e.Shard)
		assert.Equal(t, "2bcefa7dff0aed57cacbce90134434587a10c891", e.NodeID)
		assert.Equal(t, EventNode, e.Type)
		assert.Equal(t, Command(CommandUpdate), e.Command)
	}
	clusterCopy, err := stor1.GetClusterCopy("testNs", "testCluster")
	assert.Equal(t, nil, err)
	assert.Equal(t, "127.0.0.1:6379", clusterCopy.Shards[0].Nodes[0].Address)
	// read etcd
	remoteClusterCopy, err := stor1.remote.GetClusterCopy("testNs", "testCluster")
	assert.Equal(t, nil, err)
	assert.Equal(t, "127.0.0.1:6379", remoteClusterCopy.Shards[0].Nodes[0].Address)

	node.ID = "57cacbce90134434587a10c8912bcefa7dff0aed"
	node.Address = "127.0.0.1:6389"
	err = stor1.CreateNode("testNs", "testCluster", 0, node)
	assert.Equal(t, nil, err)
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, "testCluster", e.Cluster)
		assert.Equal(t, 0, e.Shard)
		assert.Equal(t, "57cacbce90134434587a10c8912bcefa7dff0aed", e.NodeID)
		assert.Equal(t, EventNode, e.Type)
		assert.Equal(t, Command(CommandCreate), e.Command)
	}
	clusterCopy, err = stor1.GetClusterCopy("testNs", "testCluster")
	assert.Equal(t, nil, err)
	assert.Equal(t, "57cacbce90134434587a10c8912bcefa7dff0aed", clusterCopy.Shards[0].Nodes[len(clusterCopy.Shards[0].Nodes) - 1].ID)
	// read etcd
	remoteClusterCopy, err = stor1.remote.GetClusterCopy("testNs", "testCluster")
	assert.Equal(t, nil, err)
	assert.Equal(t, "57cacbce90134434587a10c8912bcefa7dff0aed", remoteClusterCopy.Shards[0].Nodes[len(clusterCopy.Shards[0].Nodes) - 1].ID)

	err = stor1.RemoveNode("testNs", "testCluster", 0, "57cacbce90134434587a10c8912bcefa7dff0aed")
	assert.Equal(t, nil, err)
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, "testCluster", e.Cluster)
		assert.Equal(t, 0, e.Shard)
		assert.Equal(t, "57cacbce90134434587a10c8912bcefa7dff0aed", e.NodeID)
		assert.Equal(t, EventNode, e.Type)
		assert.Equal(t, Command(CommandRemove), e.Command)
	}
	clusterCopy, err = stor1.GetClusterCopy("testNs", "testCluster")
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(cluster.Shards[0].Nodes))
	remoteClusterCopy, err = stor1.remote.GetClusterCopy("testNs", "testCluster")
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(remoteClusterCopy.Shards[0].Nodes))

	err = stor1.RemoveCluster("testNs", "testCluster")
	assert.Equal(t, nil, err)
	select {
	case e :=<-stor1.Notify():
		assert.Equal(t, "testNs", e.Namespace)
		assert.Equal(t, "testCluster", e.Cluster)
		assert.Equal(t, EventCluster, e.Type)
		assert.Equal(t, Command(CommandRemove), e.Command)
	}
	stor1.RemoveNamespace("testNs")
}

