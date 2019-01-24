// Stuff related to network interfaces stats

package stat

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
)

// Container for stats per single interface
type Netdev struct {
	Ifname string /* interface name */
	Speed  uint32 /* interface network speed */
	Duplex uint8  /* interface duplex */
	// receive
	Rbytes      float64 /* total number of received bytes */
	Rpackets    float64 /* total number of received packets */
	Rerrs       float64 /* total number of receive errors */
	Rdrop       float64 /* total number of dropped packets */
	Rfifo       float64 /* total number of fifo buffers errors */
	Rframe      float64 /* total number of packet framing errors */
	Rcompressed float64 /* total number of received compressed packets */
	Rmulticast  float64 /* total number of received multicast packets */
	// transmit
	Tbytes      float64 /* total number of transmitted bytes */
	Tpackets    float64 /* total number of transmitted packets */
	Terrs       float64 /* total number of transmitted errors */
	Tdrop       float64 /* total number of dropped packets */
	Tfifo       float64 /* total number of fifo buffers errors */
	Tcolls      float64 /* total number of detected collisions */
	Tcarrier    float64 /* total number of carrier losses */
	Tcompressed float64 /* total number of received multicast packets */
	// enchanced
	Packets     float64 /* total number of received or transmited packets */
	Raverage    float64 /* average size of received packets */
	Taverage    float64 /* average size of transmitted packets */
	Saturation  float64 /* saturation - the number of errors/second seen for the interface */
	Rutil       float64 /* percentage utilization for bytes received */
	Tutil       float64 /* percentage utilization for bytes transmitted */
	Utilization float64 /* percentage utilization of the interface */
	Uptime      float64 /* system uptime */
}

// Container for all stats from proc-file
type Netdevs []Netdev

const (
	PROC_NETDEV = "/proc/net/dev"
)

// Read stats from local procfile source
func (c Netdevs) ReadLocal() error {
	var j = 0
	content, err := ioutil.ReadFile(PROC_NETDEV)
	if err != nil {
		return fmt.Errorf("failed to read %s", PROC_NETDEV)
	}
	reader := bufio.NewReader(bytes.NewBuffer(content))

	uptime, err := uptime()
	if err != nil {
		return err
	}
	for i := 0; i < len(c); i++ {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		if j < 2 { // skip first 2 lines - it's stats header
			j++
			i--
			continue
		}

		var ifs = Netdev{}

		_, err = fmt.Sscanln(string(line),
			&ifs.Ifname,
			&ifs.Rbytes, &ifs.Rpackets, &ifs.Rerrs, &ifs.Rdrop, &ifs.Rfifo, &ifs.Rframe, &ifs.Rcompressed, &ifs.Rmulticast,
			&ifs.Tbytes, &ifs.Tpackets, &ifs.Terrs, &ifs.Tdrop, &ifs.Tfifo, &ifs.Tcolls, &ifs.Tcarrier, &ifs.Tcompressed)
		if err != nil {
			return fmt.Errorf("failed to scan data from %s", PROC_NETDEV)
		}

		ifs.Ifname = strings.TrimRight(ifs.Ifname, ":")
		ifs.Saturation = ifs.Rerrs + ifs.Rdrop + ifs.Tdrop + ifs.Tfifo + ifs.Tcolls + ifs.Tcarrier
		ifs.Uptime = uptime

		// Get interface's speed and duplex, perhaps it's too expensive to poll interface in every execution of the function.
		ifs.Speed, ifs.Duplex, _ = GetLinkSettings(ifs.Ifname) /* use zeros if errors */

		c[i] = ifs
	}
	return nil
}

// Function returns value of particular stat of an interface
func (c Netdev) SingleStat(stat string) (value float64) {
	switch stat {
	case "rbytes":
		value = c.Rbytes
	case "rpackets":
		value = c.Rpackets
	case "rerrs":
		value = c.Rerrs
	case "rdrop":
		value = c.Rdrop
	case "rfifo":
		value = c.Rfifo
	case "rframe":
		value = c.Rframe
	case "rcompressed":
		value = c.Rcompressed
	case "rmulticast":
		value = c.Rmulticast
	case "tbytes":
		value = c.Tbytes
	case "tpackets":
		value = c.Tpackets
	case "terrs":
		value = c.Terrs
	case "tdrop":
		value = c.Tdrop
	case "tfifo":
		value = c.Tfifo
	case "tcolls":
		value = c.Tcolls
	case "tcarrier":
		value = c.Tcarrier
	case "tcompressed":
		value = c.Tcompressed
	case "saturation":
		value = c.Saturation
	case "uptime":
		value = c.Uptime
	case "speed":
		value = float64(c.Speed)
	case "duplex":
		value = float64(c.Duplex)
	default:
		value = 0
	}
	return value
}
