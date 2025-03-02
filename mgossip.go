package mgossip

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/bwmarrin/snowflake"
)

type MgossipConfig struct {
    GossipNodes        int
    RetransmitMult     int
    UDPBufferSize      int
    SystemBroadcastMult int
    ProbeInterval      int
    BindAddr           string
    AdvertiseAddr      string
    BindPort           int
    MemberlistAddr     string
    MemberlistPort     int
    Neighbors          []string
	NotifyMsg          func(msg []byte)
	MergeDelegate      func(buf []byte, join bool)
	LocalState         func(join bool) []byte
	IsMgossip          bool
}

func NewDefaultMgossipConfig() *MgossipConfig {
	return &MgossipConfig{
		GossipNodes:        2,
		RetransmitMult:     3,
		UDPBufferSize:      1400,
		SystemBroadcastMult: 1,
		ProbeInterval:      1000,
		IsMgossip:          true,
	}
}

type Mgossip struct {
    Config *MgossipConfig
	Broadcasts *TransmitLimitedQueue
	visited    map[int64]int64
	node *snowflake.Node
}

func NewMgossip(config *MgossipConfig) *Mgossip {
	snowflakeNode, err := InitSnowshake("2021-12-03", 1)
	if err != nil {
		panic(err)
	}
	mgossipInstance := &Mgossip{
		Config: config,
		Broadcasts: &TransmitLimitedQueue{
			RetransmitMult: config.RetransmitMult,
		},
		visited:    make(map[int64]int64),
		node:       snowflakeNode,
	}

	c := DefaultWANConfig()
	c.Delegate = mgossipInstance
	c.BindPort = config.BindPort
	c.BindAddr = config.BindAddr
	c.AdvertiseAddr = config.AdvertiseAddr
	c.AdvertisePort = config.BindPort
	c.Neighbors = config.Neighbors
	c.PushPullInterval = 0 
	c.GossipNodes = config.GossipNodes
	c.UDPBufferSize = config.UDPBufferSize
	c.RetransmitMult = config.SystemBroadcastMult
	c.ProbeInterval = time.Duration(config.ProbeInterval) * time.Second
	c.Name = fmt.Sprintf("%s:%d", config.AdvertiseAddr, config.BindPort)
	c.IsMgossip = config.IsMgossip
	m, err := Create(c)
	if err != nil {
		panic(err)
	}
	mgossipInstance.Broadcasts.NumNodes = func() int {
		return m.NumMembers()
	}
	if len(config.MemberlistAddr) > 0 {
		_, err := m.Join([]string{fmt.Sprintf("%s:%d", config.MemberlistAddr, config.MemberlistPort)})
		if err != nil {
			panic(err)
		}
	}
	return mgossipInstance
}

func InitSnowshake(startTime string, machineID int64) (*snowflake.Node, error){
	var st time.Time
	st, err := time.Parse("2006-01-02", startTime)
	if err != nil {
		return nil, err
	}
	snowflake.Epoch = st.UnixNano() / 1e6
	node, err := snowflake.NewNode(machineID)
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (m *Mgossip) NodeMeta(limit int) []byte {
    return []byte{}
}

func (m *Mgossip) NotifyMsg(msg []byte) {
	if len(msg) <= 15 { // drop empty message
		return 
	}
	if string(msg[: 7]) != "mgossip" {  // not mgossip msg
		return
	}
	msgID := byte2Int64(msg[7: 15])  // extract msg id
	if _, ok := m.visited[msgID]; ok {  // already visited
		return
	}
	m.Config.NotifyMsg(msg[15:])
	m.visited[msgID] = time.Now().Unix()
	m.Broadcasts.QueueBroadcast(&MgossipBroadcast{
		msg:    msg,
		notify: nil,
	})
}

func (m *Mgossip) GetBroadcasts(overhead, limit int) [][]byte {
    return m.Broadcasts.GetBroadcasts(overhead, limit)
}

func (m *Mgossip) LocalState(join bool) []byte {
    return m.Config.LocalState(join)
}

func (m *Mgossip) MergeRemoteState(buf []byte, join bool) {
    m.Config.MergeDelegate(buf, join)
}

func (m *Mgossip) BroadcastMSG(message []byte) {
	messageID := m.node.Generate().Int64()
    msg := append([]byte("mgossip"), int642Byte(messageID)...)
	m.visited[messageID] = time.Now().Unix()
	m.Broadcasts.QueueBroadcast(&MgossipBroadcast{
		msg:    append(msg, message...),
		notify: nil,
	})
}

type MgossipBroadcast struct {
    msg    []byte
    notify chan<- struct{}
}

func (b *MgossipBroadcast) Invalidates(other Broadcast) bool {
    return false
}

func (b *MgossipBroadcast) Message() []byte {
    return b.msg
}

func (b *MgossipBroadcast) Finished() {
    if b.notify != nil {
        close(b.notify)
    }
}

func byte2Int64(b []byte) int64 {
    buf := bytes.NewBuffer(b)
    var res int64
    binary.Read(buf, binary.BigEndian, &res)
    return res
}

func int642Byte(num int64) []byte {
    buf := bytes.NewBuffer([]byte{})
    binary.Write(buf, binary.BigEndian, num)
    return buf.Bytes()
}
