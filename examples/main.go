/*
 this a demo show how to use mgossip and gossip
*/

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"sync"

	"github.com/go-ini/ini"
	"github.com/zhuohuashiyi/mgossip"
)

var (
	mgossipInstance *mgossip.Mgossip

	mtx        sync.RWMutex //  读写锁
	items      = map[string]string{}
	configFile    = flag.String("conf", "config.ini", "配置文件地址")
	nodeName      = flag.String("nodeName", "firstNode", "节点名称")
	gossipNodes   = flag.Int("gossipNodes", 2, "谣言传播节点个数")
	retransmitMult = flag.Int("retransmitMult", 4, "memberlist重传次数因子")
	UDPBufferSize = flag.Int("UDPBufferSize", 1400, "UDP包的最大长度")
	systemBroadcastMult = flag.Int("systemBroadcastMult", 1, "系统广播队列的重传次数因子")
	probeInterval = flag.Int("probeInterval", 5, "probe定时")
	bindAddr string
	advertiseAddr string
	bindPort int
	memberlistAddr string
	memberlistPort int
	neighbors []string
	port int
)

// init函数负责初始化，主要是配置文件的解析和日志的设置
func init() {   // 初始化函数
	flag.Parse() //  命令行参数解析
	cfg, err := ini.Load(*configFile)
	if err != nil {
		panic("配置文件不存在或者格式有误")
	}
	bindAddr = cfg.Section(*nodeName).Key("bindAddr").String()
	advertiseAddr = cfg.Section(*nodeName).Key("advertiseAddr").String()
	bindPort, err = cfg.Section(*nodeName).Key("bindPort").Int()
	if err != nil {
		fmt.Print(err.Error())
		panic("配置文件解析失败")
	}
	memberlistAddr = cfg.Section(*nodeName).Key("memberlistAddr").String()
	memberlistPort, err = cfg.Section(*nodeName).Key("memberlistPort").Int()
	if err != nil {
		fmt.Print(err.Error())
		panic("配置文件解析失败")
	}
	neighbors = cfg.Section(*nodeName).Key("neighbors").Strings(",")
	port, err = cfg.Section(*nodeName).Key("port").Int()
	if err != nil {
		fmt.Print(err.Error())
		panic("配置文件解析失败")
	}
}


type message struct {
	Action string
	Data   map[string]string
}


// memberlist收到userMsg信息后会调用该函数处理信息
func NotifyMsg(b []byte) {
	fmt.Println("get msg1:", string(b))
	if len(b) == 0 {
		return
	}
	switch b[0] {
	case 'd':
		var m message
		if err := json.Unmarshal(b[1:], &m); err != nil {
			return
		}
		mtx.Lock()
		for k, v := range m.Data {
			switch m.Action {
			case "add":
				items[k] = v
			case "del":
				delete(items, k)
			}
		}
		mtx.Unlock()
	}
  
}


// 由push/pull协程同步交换各自的items信息
func LocalState(join bool) []byte {
	mtx.RLock()
	m := items
	mtx.RUnlock()
	b, _ := json.Marshal(m)
	return b
}

// PushPull协程将调用该函数同步节点状态
func MergeRemoteState(buf []byte, join bool) {
	if len(buf) == 0 {
		return
	}
	var m map[string]string
	if err := json.Unmarshal(buf, &m); err != nil {
		fmt.Println(err.Error())
		return
	}
	mtx.Lock()
	for k, v := range m {
		items[k] = v
	}
	mtx.Unlock()
}

func addHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	key := r.Form.Get("key")
	val := r.Form.Get("val")
	mtx.Lock()
	items[key] = val
	mtx.Unlock()
	b, err := json.Marshal(message{
		Action: "add",
		Data: map[string]string{
			key: val,
		},
	})
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	msg := append([]byte("d"), b...)
	mgossipInstance.BroadcastMSG(msg)	
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	key := r.Form.Get("key")
	mtx.RLock()
	val := items[key]
	mtx.RUnlock()
	if _, err := w.Write([]byte(val)); err != nil {
		fmt.Printf("fail to write response, err: %s.\n", err)
	}
}

func start() {
	config := &mgossip.MgossipConfig{
		BindAddr:           bindAddr,
		BindPort:           bindPort,
		AdvertiseAddr:      advertiseAddr,
		Neighbors:          neighbors,
		GossipNodes:        *gossipNodes,
		UDPBufferSize:      *UDPBufferSize,
		SystemBroadcastMult: *systemBroadcastMult,
		ProbeInterval:      *probeInterval,
		NotifyMsg:          NotifyMsg,
		LocalState:         LocalState,
		MergeDelegate:      MergeRemoteState,
		MemberlistAddr:     memberlistAddr,
		MemberlistPort:     memberlistPort,
		RetransmitMult:     *retransmitMult,
	}
	mgossipInstance = mgossip.NewMgossip(config)
}

func main() {
	start()
	http.HandleFunc("/add", addHandler)
	http.HandleFunc("/get", getHandler)
	fmt.Printf("Listening on :%d\n", port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
		panic(err)
	}
}