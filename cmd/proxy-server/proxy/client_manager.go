package proxy

import (
	"errors"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

//
type DataChannel struct {
	dataChannel net.Conn
	key         string //for the targetHost
	totalBytes  uint64
	ownerClient string //client name which data channel belongs to
}

type ExtraInfo struct {
	Longitude string `json:"longitude"`
	Latitude string `json:"latitude"`
	City string `json:"city"`
	Country string `json:"country"`
	Region string `json:"region"`
	CC string `json:"cc"`
	Platform string `json:"platform"`
}

//
type ClientInfo struct {
	sync.RWMutex
	net.Conn //客户端的控制通道连接
	Name               string `json:"name"`
	Addr               string `json:"addr"`
	Port               uint16 `json:"port"`
	TotalBytes         uint64 `json:"totalBytes"`
	ReadBuf            []byte
	WriteBuf           []byte
	ConnectionTimeInt  time.Time `json:"connectionTimeInt"`
	ConnectionTimePass float64   `json:"connectionTimePass"`
	ConnectionTime     string    `json:"connectionTime"` //客户端连接的时刻
	DataChannels       map[string]*DataChannel           //用于传输数据的数据连接通道  ---hostURL <---> DataChannel
	Location           string `json:"location"`
	CountryId          string `json:"country_id"`
	AppVersion         int    `json:"appVersion"`
	Ping               int    `json:"ping"`
	AndroidVersion     int    `json:"androidVersion"`
	IsWifi             bool   `json:"isWifi"`
	InstallId          string `json:"installId"`
	Extra              *ExtraInfo
}

//
type ClientManager struct {
	sync.RWMutex
	clientList        map[string]*ClientInfo
	totalBytes        uint64
	maxBytesInClients uint64
}

func (channel *DataChannel) GetConn() net.Conn {
	return channel.dataChannel
}

func (channel *DataChannel) GetStoryKey() string {
	return channel.key
}

//
func (c *ClientInfo) GetConn() net.Conn {
	return c.Conn
}

//
func (c *ClientInfo) GetBytes() uint64 {
	return c.TotalBytes
}

//by xh
func (c *ClientInfo) GetName() string {
	return c.Name
}

//
func NewClientMgr() *ClientManager {
	return &ClientManager{
		clientList: map[string]*ClientInfo{},
	}
}

//ip:port is ClientName
func getClientName(conn *net.Conn) string {
	return (*conn).RemoteAddr().String()
}

//
func (m *ClientManager) AddNewClient(conn *net.Conn, appVersion int, ping int, androidVersion int, isWifi bool, installId string,realIp string,extra *ExtraInfo) *ClientInfo {
	clientName := getClientName(conn)
	clientAddr := strings.Split(clientName, ":") //conn.RemoteAddr().String()
	addr := clientAddr[0]

	if realIp != "0.0.0.0" && realIp!="" {
		addr=realIp
	}

	var port string
	if len(clientAddr) == 2 {
		port = clientAddr[1]
	} else {
		port = "0"
	}
	iPort, err := strconv.Atoi(port)
	if err != nil {
		log.Printf("convert port string to int failed %v \n", port)
	}

	defer m.Unlock()
	m.Lock()
	_, ok := m.clientList[clientName]
	if ok {
		log.Printf("client(%s) has exists !!!", clientName)
		//if m.clientList[clientName].ConnectionTimeInt <=0{
		//	m.clientList[clientName].ConnectionTime = time.Now()
		//}
		return nil
	}

	now := time.Now()
	nowTime := now.Format("2006-01-02 15:04:05")
	clientInfo := &ClientInfo{
		Conn:              *conn,
		Name:              clientName,
		Addr:              addr,
		Port:              uint16(iPort),
		ConnectionTime:    nowTime,
		ConnectionTimeInt: now,
		TotalBytes:        0,
		AppVersion:        appVersion,
		AndroidVersion:    androidVersion,
		Ping:              ping,
		IsWifi:            isWifi,
		InstallId:         installId,
		Extra:             extra,
	}
	m.clientList[clientName] = clientInfo
	return clientInfo
}

//host format: 10.1.23.45:port or  www.google.com:443
func NewDataChannel(conn net.Conn, storyKey string, clientName string) *DataChannel {
	return &DataChannel{
		dataChannel: conn,
		key:         storyKey,
	}
}

//conn: client control connection
func (m *ClientManager) AddDataChannel(clientName string, hostURL string, dataChannel *DataChannel) error {
	defer m.RUnlock()
	m.RLock()
	if _, ok := m.clientList[clientName]; !ok {
		log.Printf("client control channel is not exists %s\n", clientName)
		return errors.New("control channel is not exists")
	}
	client, _ := m.clientList[clientName]
	key := dataChannel.GetStoryKey()

	client.Lock()  //1107
	defer client.Unlock() //1107
	if _, ok2 := client.DataChannels[key]; ok2 {
		log.Printf("data channel is exists %s, update it\n", key)
		client.DataChannels[key] = dataChannel
		return nil
	}

	if client.DataChannels == nil {
		client.DataChannels = make(map[string]*DataChannel)
	}

	client.DataChannels[key] = dataChannel
	return nil
}

//conn , shadow conn from shadow server
func (m *ClientManager) RemoveDataChannel(clientName string, conn net.Conn, dataChannelConn net.Conn, storyKey string) {
	//clientName := getClientName(&conn)
	log.Printf("clientName is %s", clientName)
	defer m.RUnlock()
	m.RLock()

	if _, ok := m.clientList[clientName]; !ok {
		log.Printf("datachannel is not exists %s\n", clientName)
		return
	}
	defer m.clientList[clientName].Unlock()
	m.clientList[clientName].Lock()
	for k, v := range m.clientList[clientName].DataChannels {
		if storyKey == k {
			delete(m.clientList[clientName].DataChannels, k)
			log.Printf("removed channel to %s clientName is %s \n", v.GetStoryKey(), clientName)
		}
	}

}

func (m *ClientManager) GetConnByClientName(clientName string) net.Conn{
	defer m.RUnlock()
	m.RLock()
	if _,ok:=m.clientList[clientName];ok{
		return m.clientList[clientName].Conn
	}
	return nil
}

//
func (m *ClientManager) DelClient(conn net.Conn) {
	defer m.Unlock()
	m.Lock()
	clientName := getClientName(&conn)
	delete(m.clientList,clientName)
	//for _, v := range m.clientList {
	//	if getClientName(&v.Conn) == clientName {
	//		delete(m.clientList,v.Name)
	//		break
	//	}
	//}
	return
}

//
func (m *ClientManager) UpdateClientBytes(bytes uint64, clientName string) {
	defer m.RUnlock()
	m.RLock()
	if _, ok := m.clientList[clientName]; ok {
		m.clientList[clientName].TotalBytes += bytes
	} else {
		log.Printf("update client %s error , because client is not exists \n", clientName)
	}

}

func (m *ClientManager) UpdateClientDataChanBytes(bytes uint64, conn net.Conn) {
	defer m.RUnlock()
	m.RLock()
	for _, v := range m.clientList {
		v.RLock() //1107
		for _, vc := range v.DataChannels {
			if getClientName(&vc.dataChannel) == getClientName(&conn) {
				vc.totalBytes += bytes
				m.totalBytes += bytes
				break
			}
		}
		v.RUnlock() //1107
	}
}


func (m *ClientManager) GetClientsCount() int {
	defer m.RUnlock()
	m.RLock()
	return len(m.clientList)
}

func (m *ClientManager) GetClientsArr(cc string,platform string) []string {
	defer m.RUnlock()
	m.RLock()
	keys := make([]string, 0)
	for _, v := range m.clientList {
		if  cc != "" {
			if v.Extra.CC != cc{
				continue
			}
		}
		if  platform != "" {
			if v.Extra.Platform != platform{
				continue
			}
		}
		keys = append(keys, v.Addr)
	}
	return keys
}
//
func (m *ClientManager) EnumClients(callback func(c *ClientInfo)) {
	defer m.RUnlock()
	m.RLock()
	for _, v := range m.clientList {
		callback(v)
	}
}


//func (m *ClientManager) GetClients() map[string]*ClientInfo {
//	return m.clientList;
//}
