package shadowsocks

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"net"
	"time"
	"sync"
)

func SetReadTimeout(c net.Conn) {
	//if readTimeout != 0 {
	//	log.Printf("set read time out %d \n", readTimeout)
	//	c.SetReadDeadline(time.Now().Add(readTimeout))
	//}
	c.SetReadDeadline(time.Now().Add(time.Duration(240 * time.Second)))

}

// PipeThenClose copies data from src to dst, closes dst when done.
func PipeThenClose(src, dst net.Conn, addFlow func(int)) {
	//log.Printf("pipe data between %s <-->%s \n", src.RemoteAddr().String(), dst.RemoteAddr().String())
	defer dst.Close()
	buf := leakyBuf.Get()
	defer leakyBuf.Put(buf)
	for {
		SetReadTimeout(src)
		n, err := src.Read(buf)
		if addFlow != nil {
			addFlow(n)
		}
		// read may return EOF with n > 0
		// should always process n > 0 bytes before handling error
		if n > 0 {
			// Note: avoid overwrite err returned by Read.
			//log.Printf("write data to %s size %d \n", dst.RemoteAddr().String(), n)
			if _, err := dst.Write(buf[0:n]); err != nil {
				Debug.Println("write:", err)
				break
			}
		}
		if err != nil {
			// Always "use of closed network connection", but no easy way to
			// identify this specific error. So just leave the error along for now.
			// More info here: https://code.google.com/p/go/issues/detail?id=4373

			if bool(Debug) && err != io.EOF {
				Debug.Println("read:", err)
			}

			log.Printf("pipe data error %s, quit pipe \n", err.Error())
			break
		}
	}
	return
}

var mapClientPipeErrorTimes = make(map[string]int)
var lock sync.Mutex
//func updateClientError(clientName string){
//	//lock.Lock()
//	//defer lock.Unlock()
//	if _,ok:=mapClientPipeErrorTimes[clientName];ok{
//		mapClientPipeErrorTimes[clientName] = mapClientPipeErrorTimes[clientName] + 1
//	}else{
//		mapClientPipeErrorTimes[clientName] = 1
//	}
//}
func PipeThenCloseNew(src, dst net.Conn, addFlow func(int), targetHost string, clientName string, closeConn func(client string)) {
	log.Printf("pipe data between %s <-->%s %s \n",
		src.RemoteAddr().String(), dst.RemoteAddr().String(), targetHost)

	defer func() {
		log.Printf("pipe data between %s <-->%s %s closed\n",
			src.RemoteAddr().String(), dst.RemoteAddr().String(), targetHost)
	}()
	defer dst.Close()
	buf := leakyBuf.Get()
	defer leakyBuf.Put(buf)
	for {
		SetReadTimeout(src)

		//src.SetReadDeadline(nowTime.Add(time.Duration(130 * time.Second)))
		n, err := src.Read(buf)
		if addFlow != nil {
			addFlow(n)
		}
		// read may return EOF with n > 0
		// should always process n > 0 bytes before handling error
		if n > 0 {
			// Note: avoid overwrite err returned by Read.
			log.Printf("pipe data %s ==> %s size %d == %s\n", src.RemoteAddr().String(), dst.RemoteAddr().String(), n, targetHost)
			nowTime := time.Now()
			dst.SetWriteDeadline(nowTime.Add(time.Duration(240 * time.Second)))
			if _, err := dst.Write(buf[0:n]); err != nil {
				log.Printf("pipe write error:", err)
				//updateClientError(clientName)
				break
			}
		}
		//lock.Lock()
		//defer lock.Unlock()
		if err != nil {
			// Always "use of closed network connection", but no easy way to
			// identify this specific error. So just leave the error along for now.
			// More info here: https://code.google.com/p/go/issues/detail?id=4373
			/*
				if bool(Debug) && err != io.EOF {
					Debug.Println("read:", err)
				}
			*/
			//updateClientError(clientName)

			//if (mapClientPipeErrorTimes[clientName] >= 4) {
			//	log.Printf("pipe data error >=4,so close this clientName %s, quit pipe , host %s clientName is %s  \n", err.Error(), targetHost, clientName)
			//	delete(mapClientPipeErrorTimes, clientName)
			//	closeConn(clientName)
			//}
			log.Printf("pipe data error new %s, quit pipe , host %s clientName is %s  \n", err.Error(), targetHost, clientName)
			break
		} else {
			mapClientPipeErrorTimes[clientName] = 0
		}
	}
}


func PipeThenCloseOld(src, dst net.Conn, addFlow func(int), targetHost string) {
	log.Printf("pipe data between %s <-->%s %s \n",
		src.RemoteAddr().String(), dst.RemoteAddr().String(), targetHost)

	defer func() {
		log.Printf("pipe data between %s <-->%s %s closed\n",
			src.RemoteAddr().String(), dst.RemoteAddr().String(), targetHost)
	}()
	defer dst.Close()
	buf := leakyBuf.Get()
	defer leakyBuf.Put(buf)
	for {
		SetReadTimeout(src)
		n, err := src.Read(buf)
		if addFlow != nil {
			addFlow(n)
		}
		// read may return EOF with n > 0
		// should always process n > 0 bytes before handling error
		if n > 0 {
			// Note: avoid overwrite err returned by Read.
			log.Printf("pipe data %s ==> %s size %d == %s\n", src.RemoteAddr().String(), dst.RemoteAddr().String(), n , targetHost)
			dst.SetWriteDeadline(time.Now().Add(time.Duration(240 * time.Second)))
			if _, err := dst.Write(buf[0:n]); err != nil {
				Debug.Println("write:", err)
				break
			}
		}
		if err != nil {
			// Always "use of closed network connection", but no easy way to
			// identify this specific error. So just leave the error along for now.
			// More info here: https://code.google.com/p/go/issues/detail?id=4373

			if bool(Debug) && err != io.EOF {
				Debug.Println("read:", err)
			}

			log.Printf("pipe data error old %s, quit pipe , host %s  %s <-> %s \n", err.Error(), targetHost,src.RemoteAddr().String(), dst.RemoteAddr().String())
			break
		}
	}
}


// PipeThenClose copies data from src to dst, closes dst when done, with ota verification.
func PipeThenCloseOta(src *Conn, dst net.Conn, addFlow func(int)) {
	const (
		dataLenLen  = 2
		hmacSha1Len = 10
		idxData0    = dataLenLen + hmacSha1Len
	)

	defer func() {
		dst.Close()
	}()
	// sometimes it have to fill large block
	buf := leakyBuf.Get()
	defer leakyBuf.Put(buf)
	for i := 1; ; i += 1 {
		SetReadTimeout(src)
		if n, err := io.ReadFull(src, buf[:dataLenLen+hmacSha1Len]); err != nil {
			if err == io.EOF {
				break
			}
			Debug.Printf("conn=%p #%v read header error n=%v: %v", src, i, n, err)
			break
		}
		dataLen := binary.BigEndian.Uint16(buf[:dataLenLen])
		expectedHmacSha1 := buf[dataLenLen:idxData0]

		var dataBuf []byte
		if len(buf) < int(idxData0+dataLen) {
			dataBuf = make([]byte, dataLen)
		} else {
			dataBuf = buf[idxData0 : idxData0+dataLen]
		}
		if n, err := io.ReadFull(src, dataBuf); err != nil {
			if err == io.EOF {
				break
			}
			Debug.Printf("conn=%p #%v read data error n=%v: %v", src, i, n, err)
			break
		}
		addFlow(int(dataLen))
		chunkIdBytes := make([]byte, 4)
		chunkId := src.GetAndIncrChunkId()
		binary.BigEndian.PutUint32(chunkIdBytes, chunkId)
		actualHmacSha1 := HmacSha1(append(src.GetIv(), chunkIdBytes...), dataBuf)
		if !bytes.Equal(expectedHmacSha1, actualHmacSha1) {
			Debug.Printf("conn=%p #%v read data hmac-sha1 mismatch, iv=%v chunkId=%v src=%v dst=%v len=%v expeced=%v actual=%v", src, i, src.GetIv(), chunkId, src.RemoteAddr(), dst.RemoteAddr(), dataLen, expectedHmacSha1, actualHmacSha1)
			break
		}
		if n, err := dst.Write(dataBuf); err != nil {
			Debug.Printf("conn=%p #%v write data error n=%v: %v", dst, i, n, err)
			break
		}
	}
	return
}
