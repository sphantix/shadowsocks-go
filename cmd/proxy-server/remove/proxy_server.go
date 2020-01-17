package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/satori/go.uuid"
	proxy "github.com/shadowsocks/shadowsocks-go/cmd/proxy-server/proxy"
	ss "github.com/shadowsocks/shadowsocks-go/shadowsocks"
)

const (
	idAddrTypeIndex = 0
	idAddrLenIndex  = 1
	idIP0           = 1 // ip addres start index
	typeIPv4        = 1 // type is ipv4 address
	typeDm          = 3 // type is domain address
	typeIPv6        = 4 // type is ipv6 address
	idDm0           = 2 // domain address start index

	lenIPv4   = net.IPv4len + 2 // ipv4 + 2port
	lenIPv6   = net.IPv6len + 2 // ipv6 + 2port
	lenDmBase = 2               // 1addrLen + 2port, plus addrLen
	idDmLen   = 1

	regCtrlChan    = 0 //注册控制通道请求
	connectTarget  = 1 //连接目标URL 命令
	regOk          = 2 //告知客户端他的名字
	imReady        = 3 //客户端准备好连接目标了
	readyToConnect = 4 //客户端数据通道连接上来了。告诉服务器自己的名字+目标url
	heartbeatReq   = 5 //心跳
	heartbeatRep   = 6 //心跳响应
	appIdRequest = 8
)

var printVer = false
var localBindAddr = "127.0.0.1"
var localBindPort = 8387

var localBindAddrForClient string
var localBindPortForClient int
var localBindPortForClientData int
func waitSignal() {
	var sigChan = make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP)
	for sig := range sigChan {
		if sig == syscall.SIGHUP {
			log.Printf("i was kill by sigup ")
		} else {
			// is this going to happen?
			log.Printf("caught signal %v, exit", sig)
			os.Exit(0)
		}
	}
}

func getRequest(conn *net.Conn) (host string, port uint16, rawAddr []byte, appId string, err error) {

	for i := 0 ; i < 2; i++ {
		nowTime := time.Now()
		(*conn).SetReadDeadline(nowTime.Add(time.Duration(10 * time.Second)))
		buf := make([]byte, 269)
		//read add type
		_, err1 := io.ReadFull((*conn), buf[:1])
		if err1 != nil {
			log.Printf("read shadow request failed %v", err1.Error())
			err = errors.New("read error")
			return
		}
	
		var reqStart int
		var reqEnd int
		addrType := buf[idAddrTypeIndex]
		switch addrType & 0xf {
		case typeIPv4:
			reqStart = idAddrTypeIndex + 1
			reqEnd = idAddrTypeIndex + 1 + lenIPv4
		case typeIPv6:
			reqStart = idAddrTypeIndex + 1
			reqEnd = idAddrTypeIndex + 1 + lenIPv6
		case typeDm:
			//read length field
			if _, err1 = io.ReadFull((*conn), buf[idAddrTypeIndex+1:idDmLen+1]); err1 != nil {
				log.Printf("read request error \n")
				err = errors.New("read request format error")
				return
			}
			log.Printf("read domain len %d \n", buf[idAddrTypeIndex+1:idDmLen+1])
			reqStart, reqEnd = idDm0, idDm0+int(buf[idDmLen])
		case appIdRequest:
			if _, err2 := io.ReadFull((*conn), buf[1:2]); err2 != nil {
				log.Printf("read request error \n")
				err = errors.New("read request error")
				return
			}
			appIdLen :=  buf[1]
			if int(appIdLen) <= 0 {
				log.Printf("appid len error %d \n", appIdLen)
				return
			}
			var readLen int
			var err3 error
			if readLen, err3 = io.ReadFull((*conn), buf[2:appIdLen + 2]); err3 != nil {
				log.Printf("read AppId Failed \n")
				return
			}
			if readLen != int(appIdLen) {
				log.Printf("read appid len is not equal with actually \n")
				return
			}
			appId = string(buf[2:appIdLen + 2])
			return
		default:
			log.Printf("read request format error %v", buf)
			err = errors.New("read request format error")
			return
		}
	
		_, err1 = io.ReadFull((*conn), buf[reqStart:reqEnd])
		if err1 != nil {
			log.Printf("read left request failed %v ", err.Error())
			err = errors.New("read left request failed")
			return
		}
	
		rawAddr = buf[:reqEnd]
		if addrType&0xf == typeDm {
			log.Printf("read domain  %s \n", string(buf[reqStart:reqEnd]))
		} else {
			log.Printf("read ipv4  %x \n", buf[reqStart:reqEnd])
		}
	
		// Return string for typeIP is not most efficient, but browsers (Chrome,
		// Safari, Firefox) all seems using typeDm exclusively. So this is not a
		// big problem.
		switch addrType & 0xf {
		case typeIPv4:
			host = net.IP(buf[idIP0 : idIP0+net.IPv4len]).String()
			port = binary.BigEndian.Uint16(buf[reqEnd-2 : reqEnd])
		case typeIPv6:
			host = net.IP(buf[idIP0 : idIP0+net.IPv6len]).String()
		case typeDm:
			for i := idDm0; i < idDm0+int(buf[idDmLen]); i++ {
				if buf[i] == ':' {
					var iPort int
					host = string(buf[idDm0:i])
					iPort, err = strconv.Atoi(string(buf[i+1 : idDm0+int(buf[idDmLen])]))
					if err != nil {
						log.Printf("parse port failed for \n")
						return
					}
					port = uint16(iPort)
				}
			}
			//host = string(buf[idDm0:(idDm0 + int(buf[idDmLen]))])
		}
	}
	

	//port = binary.BigEndian.Uint16(buf[reqEnd-2 : reqEnd])
	//host = net.JoinHostPort(host, strconv.Itoa(int(port)))

	return

}

func getBestClient(appId string) *proxy.ClientInfo {
	var bestClient *proxy.ClientInfo
	var minMumBytes uint64
	log.Printf("get client for appid %s \n", appId)
	gClientManager.EnumClients(func(c *proxy.ClientInfo) {
		if minMumBytes == 0 && c.GetBytes() > 0 {
			minMumBytes = c.GetBytes()
		}
		if c.GetBytes() <= minMumBytes {
			minMumBytes = c.GetBytes()
			bestClient = c
			log.Printf("get client : %s \n", c.RemoteAddr().String())
		}
	})
	return bestClient
}

//这里需要告知客户端需要重新连接到服务器上来，用这个新的连接来中转数据
//客户端和服务器只需要一个长连接来接收服务器的指令即可
//当新的用于中转数据的连接，连接上了之后，客户端主动上报自己的名字来标明自己的身份,同时上报自己的需要连接的目标主机URL
//然后让服务器端知道是哪个客户端的连接。这样才把
//数据中转给这个连接

//控制通道的命令格式
/*
|Cmd(1byte)|Len(2bytes)|data(n bytes)
Len 是指后面的数据的长度

ProxyServer <---> Client
control channel establish:
1. client ----connect ---> server
2. client ----register(Cmd 0)--->server //by control connection
3. server---send the Name to Client(Cmd 2)--> Client .
client save this name as his Name(Public IP: port) //by control connection

data channel establish:
1. server ---> please connect to target url(Cmd connectTarget ) ---> client //by control connection
2. client ---- ok, i will connect the url(Cmd imReady)--> server //by control connection


3. client ---new connection to server (data channel) ---> server //by data channel
4. client ---tel the server the Name of client + targetUrl (Cmd readyToConnect)-->server //by data channel
5. pipe data between data channel and target url channel

Cmd: 0 regCtrlChan ---注册控制连接  0x00 0x00
Cmd: 1 connectTarget ---连接新的目的地址（rawaddr addrType url:port or ip:port） 1 xx google.com:443  代理服务器通过控制通道告诉客户端去连接目标地址
Cmd: 2 regOk ---代理服务器通过控制通道告诉客户端他控制通道的公网IP:PORT地址 此命令是对Cmd 0的响应
Cmd: 3 imReady ---客户端告诉代理服务器自己可以连接目标地址了
Cmd: 4 readyToConnect ---客户端回传自己的控制通道的公网的IP:PORT + 连接的targetUrl 0x04  172.27.37.13:24532@targetHost  url

客户端的数据通道第一连接上来之后就会发送Cmd : 3, 然后发送 Cmd 4,
这样第一次命令用于告诉代理服务器需要关联当前的数据通道到哪个控制客户端的数据结构
第二个命令用于告诉代理服务器当前数据通道关联的目标URL
*/

/*
regCtrlChan    = 0 //注册控制通道请求
connectTarget  = 1 //连接目标URL 命令
regOk       = 2 //告知客户端他的名字
imReady        = 3 //客户端准备好连接目标了
readyToConnect = 4 //客户端数据通道连接上来了。告诉服务器自己的名字+目标url
*/

//ip:port is ClientName
func getConnName(conn net.Conn) string {
	return conn.RemoteAddr().String()
}

//conn 客户端的控制通道连接
//type(1):Len(2):NameString(eg:221.23.123.42:9822)
func buildRegCtrlChanOk(conn net.Conn) []byte {
	clientName := getConnName(conn)

	buffer := make([]byte, 1+2+len(clientName))
	buffer[0] = regOk
	binary.BigEndian.PutUint16(buffer[1:3], uint16(len(clientName)))
	copy(buffer[3:], []byte(clientName))
	return buffer
}

//type(1):Len(2):rawaddr(eg:google.com:443)@shadowConn(127.0.0.1:2331) @uuid

//这个协议的报文格式是:
//type(1)+Len(2)+rawaddr(eg:google.com:443)+@+shadowConn(127.0.0.1:2331)+@+uuid
//加uuid是为了保证每次从shadowsocks过来的请求的唯一性
//而其中rawaddr的格式为:
//addrType(1)+addrLen(1)+address
//返回发送的包和存入容器的key值
func buildConnectTarget(rawAddr []byte, shadowConn net.Conn) ([]byte, string) {
	shadowAddr := shadowConn.RemoteAddr().String()
	u1, err := uuid.NewV4()
	if err != nil {
		log.Printf("create uuid failed for connect to target %s \n", err.Error())
		return make([]byte, 1), ""
	}
	uuid := u1.String()
	key := shadowAddr + string('@') + uuid
	//totallen:3 + rawaddr + @ + shadowaddr + @+ uuid
	//dataLen : rawaddr + @ + shadowaddr + @ + uuid
	dataLen := uint16(len(rawAddr) + 1 + len([]byte(shadowAddr)) + 1 + len(uuid))
	buffer := make([]byte, 1+2+dataLen)
	offset := 0
	buffer[offset] = connectTarget
	offset += 1
	binary.BigEndian.PutUint16(buffer[offset:3], dataLen) //写入数据长度信息
	offset += 2
	copy(buffer[offset:], rawAddr)
	offset += len(rawAddr)
	copy(buffer[offset:offset+1], string('@'))
	offset += 1
	copy(buffer[offset:], shadowAddr)
	offset += len([]byte(shadowAddr))
	copy(buffer[offset:offset+1], string('@'))
	offset += 1
	copy(buffer[offset:], uuid)
	offset += len(uuid)
	//log.Printf("CONN_TO_TARGET len %d  data %v \n", offset, buffer)

	return buffer, key
}

func buildHeartbeatReq() []byte {
	buffer := make([]byte, 1)
	buffer[0] = heartbeatReq
	return buffer
}

func buildHeartbeatRep() []byte {
	buffer := make([]byte, 1)
	buffer[0] = heartbeatRep
	return buffer
}

//处理来此shadowsocks 的新的连接请求
//存储shadow socks连接的key 为shadowConn(127.0.0.1:2331)+@+uuid
func handleNewConnRequest(conn net.Conn, client *proxy.ClientInfo, rawAddr []byte, 
	targetHost string, appId string) {
	writeData, key := buildConnectTarget(rawAddr, conn) //key <===> shadowConn(127.0.0.1:2331)+@+uuid
	_, err := client.Conn.Write(writeData)              //向客户端的控制通道写入命令 connectTarget
	if err != nil {
		log.Printf("write raw addr to client failed")
		conn.Close()
		return
	}
	log.Printf("write CONN_TO_TARGET to client %s  %s  appId %s\n", 
		getConnName(client.Conn), string(writeData), appId)
	
	storyKey := targetHost + string('@') + key //targetHost + @ +shadowConn(127.0.0.1:2331)+@+uuid
	shadowChannel := proxy.NewShadowChannel(targetHost, storyKey, conn, appId)
	log.Printf("add shadow channel with key : %s \n", storyKey)
	gShadowChanManager.AddShadowChannel(shadowChannel)
	//log.Printf("waiting for READY_TO_CONN command on DataChannel for  : %s ...\n", storyKey)

}

//connection from the shadowsocks server
func handleNewConn(conn net.Conn) {
	if debug {
		log.Printf("handle request from shadow server %s \n", conn.RemoteAddr().String())
	}
	host, port, rawAddr, appId, err := getRequest(&conn)
	if err != nil {
		log.Printf("get request failed \n")
		conn.Close()
		return
	}

	if debug {
		log.Printf("get target addr( %s:%d)  raw addr %v from shadowserver\n", host, port, rawAddr)
	}

	bestClient := getBestClient(appId)
	if bestClient == nil {
		log.Printf("can not find best client for %s \n", host)
		conn.Close()
		return
	}
	targetHost := net.JoinHostPort(host, strconv.Itoa(int(port)))

	go handleNewConnRequest(conn, bestClient, rawAddr, targetHost, appId)

}

var gClientManager = proxy.NewClientMgr()
var gShadowChanManager = proxy.NewShadowChanMgr()

//每120秒给控制连接发送心跳，防止控制连接断开
func handleClientControlChannel(conn *net.Conn) {
	//说明是一个控制连接
	log.Printf("new client control channel coming \n")
	regOk := buildRegCtrlChanOk(*conn)
	(*conn).Write(regOk)

	log.Printf("write reg ok to client %s \n", getConnName((*conn)))
	gClientManager.AddNewClient(conn)
	defer func() {
		gClientManager.DelClient(*conn)
		(*conn).Close()
		log.Printf("remove client control channel %s\n", getConnName((*conn)))
	}()
	
	var loopTimes int64
	for {
		loopTimes++
		time.Sleep(time.Second * 30)
		if loopTimes % 2 == 1 {
			heartBeatReqPkt := buildHeartbeatReq()
			n, err := (*conn).Write(heartBeatReqPkt)
			if err != nil {
				log.Printf("write heartbeat req failed %s , Client disconnected \n", getConnName((*conn)))
				break
			}
			log.Printf("write heartbeat to client %s \n", getConnName((*conn)))
			gClientManager.UpdateClientBytes(uint64(n), getConnName(*conn))
			buf := make([]byte, 2)
			(*conn).SetReadDeadline(time.Now().Add(time.Second * 5))
			readed, err2 := io.ReadAtLeast((*conn), buf, 1)
			if err2 != nil {
				log.Printf("read heartbeat rep failed %s , Client disconnected \n", getConnName((*conn)))
				break
			}
			if int(buf[0]) == heartbeatRep {
				log.Printf("get heartbeat response from client %s \n", getConnName((*conn)))
				gClientManager.UpdateClientBytes(uint64(readed), getConnName(*conn))
			} else if int(buf[0]) == imReady {
				log.Printf("get IMREADY from client %s \n", getConnName((*conn)))
			}
		}
	}
}

//connection from the client which is a client computer or mobile phone
// conn , data channel from client
func handleNewClientConn(conn *net.Conn) {
	nowTime := time.Now()
	(*conn).SetReadDeadline(nowTime.Add(time.Duration(10 * time.Second)))
	buf := make([]byte, 1024)
	n, err1 := io.ReadAtLeast((*conn), buf, 2)
	if err1 != nil {
		log.Printf("read request failed %v", err1.Error())
		(*conn).Close()
		return
	}

	if n == 2 {
		if buf[0] == regCtrlChan && buf[1] == 0 {
			log.Printf("get register request from client : %s \n", (*conn).RemoteAddr().String())
			go handleClientControlChannel(conn)
		} else {
			log.Printf("unknow request from client %v  addr : %s", buf, (*conn).RemoteAddr().String())
			(*conn).Close()
		}
	} else if n < 2 {
		log.Printf("unknow request from client %v  addr : %s", buf, (*conn).RemoteAddr().String())
		(*conn).Close()
	} else {
		log.Printf("data channel connect to wrong port %s \n", (*conn).RemoteAddr().String())
	}

}

func listenForClient() {
	listenAddr := net.JoinHostPort(localBindAddrForClient, strconv.Itoa(localBindPortForClient))
	listenSock, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Printf("listen on local for client failed %v", err)
		return
	}
	defer func() {
		listenSock.Close()
	}()

	log.Printf("listen on addr for client %s \n", listenSock.Addr().String())
	for {
		newConn, err1 := listenSock.Accept()
		if err1 != nil {
			log.Printf("accept error for client ")
			return
		}
		log.Printf("new client control channel from  %s \n", newConn.RemoteAddr().String())
		go handleNewClientConn(&newConn)

	}

}

//client will connect data channel to this port 8879
func handleNewClientDataConn(conn *net.Conn) {
		nowTime := time.Now()
		(*conn).SetReadDeadline(nowTime.Add(time.Duration(10 * time.Second)))
		buf := make([]byte, 1024)
		n, err1 := io.ReadAtLeast((*conn), buf, 2)
		if err1 != nil {
			log.Printf("read request failed %v", err1.Error())
			(*conn).Close()
			return
		}
	
		if n == 2 {
			log.Printf("eror request from client . because this is data channel port %s  \n", (*conn).RemoteAddr().String())
		} else if n < 2 {
			log.Printf("unknow request from client %v  addr : %s", buf, (*conn).RemoteAddr().String())
			(*conn).Close()
		} else {
			log.Printf("data channel coming ... \n")
			//说明是数据通道
			//format 1byte(type) 2bytes(len) +myname(110.185.57.208:2509) + @ + rawaddr + @ +shadowsockaddr+ @+uuid
			//format: 1byte(type)+2bytes(len)+myname(110.185.57.208:2509) + @ + rawaddr+ @ + shadowsockaddr +@+uuid
			if buf[0] == readyToConnect {
				//0x04    172.27.37.13:24532@rawaddr   // rawaddr(addrType(1) len(1) data)
				//说明客户端端已经准备好连接了
	
				log.Printf("recv READY_TO_CONN  len %d  %s\n", n, buf[:n])
				//log.Printf("READY_TO_CONN bytes len %x \n", buf[:n])
				dataLen := binary.BigEndian.Uint16(buf[1:3])
				if dataLen <= 2 {
					log.Printf("data length error (%v) \n", buf)
					(*conn).Close()
					return
				}
	
				if n < int(dataLen)+3 {
					_, err := io.ReadFull((*conn), buf[n:])
					if err != nil {
						log.Printf("read readToConnect data failed  %s \n", err.Error())
						(*conn).Close()
						return
					}
				}
	
				//log.Printf("data len %d \n", dataLen)
				totalLen := 1 + 2 + dataLen
				if int(totalLen) != n {
					log.Printf("data len is error readed:%d calculte:%d \n", n, totalLen)
				}
	
				var pos int
				//	找到clientName与rawaddr @的位置
				for i := 3; i < int(totalLen); i++ {
					if buf[i] == '@' {
						pos = i
						break
					}
				}
				clientName := string(buf[3:pos])
				log.Printf("get client name is %s \n", clientName)
				targetHostType := buf[pos+1]
				var targetHost string
				var targetPort string
				var posSep int
				var posDollar int
				var key string
				switch targetHostType {
				case typeIPv4:
					log.Printf("target host type is IPV4\n")
					targetHost := string(buf[pos+2 : totalLen-2])
					targetPort = strconv.Itoa(int(binary.BigEndian.Uint16(buf[totalLen-2 : totalLen])))
					targetHost = net.JoinHostPort(targetHost, targetPort)
					break
				case typeIPv6:
					log.Printf("not support IPV6 addr \n")
					(*conn).Close()
					return
				case typeDm:
					log.Printf("target host type is domain\n")
					for i := pos + 1; i < int(totalLen); i++ {
						if buf[i] == ':' {
							posSep = i
							//log.Printf("found domain : index is %d \n", posSep)
							break
						}
					}
	
					//找到rawaddr--- @ ---shadowsockaddr+@+uuid 之间的@符号的位置
					for i := posSep; i < int(totalLen); i++ {
						if buf[i] == '@' {
							posDollar = i
						}
					}
					targetHost = string(buf[pos+1+2 : posSep])
					targetPort = string(buf[posSep+1 : posDollar])
					key = string(buf[posDollar+1 : n])
					targetHost = net.JoinHostPort(targetHost, targetPort)
					break
				default:
					log.Printf("error addr type %d \n", targetHostType)
					break
				}
				storyKey := targetHost + string('@') + key
				log.Printf("find target host is:%s  storykey %s \n", targetHost, storyKey)
	
				shadowChanInfo := gShadowChanManager.FindShadowChannel(storyKey)
				if shadowChanInfo == nil {
					log.Printf("can not find shadow channel for host %s \n", storyKey)
					(*conn).Close()
					return
				}
	
				dataChannel := proxy.NewDataChannel(*conn, storyKey, clientName)
				if gClientManager.AddDataChannel(clientName, storyKey, dataChannel) != nil {
					log.Printf("add data channel error !")
					(*conn).Close()
					return
				}
	
				go ss.PipeThenClose2(*conn, shadowChanInfo.GetConn(), func(bytes int) {
					gClientManager.UpdateClientBytes(uint64(bytes), clientName)
				}, targetHost)
	
				ss.PipeThenClose2(shadowChanInfo.GetConn(), *conn, func(bytes int) {
					gClientManager.UpdateClientBytes(uint64(bytes), clientName)
				}, targetHost)
	
				//shadow conn <---> client data channel
				gClientManager.RemoveDataChannel(shadowChanInfo.GetConn(), *conn, storyKey)
				log.Printf("remove data channel for host %s \n", targetHost)
	
			} else {
				log.Printf("unknow request from client %v  addr : %s", buf, (*conn).RemoteAddr().String())
				(*conn).Close()
			}
		}

}


func listenForClientData() {
	listenAddr := net.JoinHostPort(localBindAddrForClient, strconv.Itoa(localBindPortForClientData))
	listenSock, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Printf("listen on local for client failed %v", err)
		return
	}
	defer func() {
		listenSock.Close()
	}()

	log.Printf("listen on addr for client data channel %s \n", listenSock.Addr().String())
	for {
		newConn, err1 := listenSock.Accept()
		if err1 != nil {
			log.Printf("accept error for client ")
			return
		}
		log.Printf("new client conn from  %s \n", newConn.RemoteAddr().String())
		go handleNewClientDataConn(&newConn)

	}
}



func listenForManager() {

}

var debug = true

func main() {
	log.SetOutput(os.Stdout)
	flag.BoolVar(&printVer, "version", false, "print version")
	flag.StringVar(&localBindAddr, "bind-addr", "127.0.0.1", " specify local bind ip address")
	flag.IntVar(&localBindPort, "bind-port", 8877, "specify local bind port")

	flag.StringVar(&localBindAddrForClient, "bind-addr-for-client", "0.0.0.0", " specify local bind ip address for client connection")
	flag.IntVar(&localBindPortForClient, "bind-port-for-client", 8878, "specify local bind port for client connection")
	flag.IntVar(&localBindPortForClientData, "bind-port-for-client-data", 8879, "specify local bind port for client data")

	listenAddr := net.JoinHostPort(localBindAddr, strconv.Itoa(localBindPort))
	listenSock, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Printf("listen on local failed %v", err)
		return
	}
	defer func() {
		listenSock.Close()
	}()

	go listenForClient()
	go listenForClientData()
	go listenForManager()

	log.Printf("listen on addr for shadow, Proxy addr %s \n", listenSock.Addr().String())
	for {
		newConn, err2 := listenSock.Accept()
		if err2 != nil {
			log.Printf("accept error ")
			return
		}
		go handleNewConn(newConn)
	}
}
