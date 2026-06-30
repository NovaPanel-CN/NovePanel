package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	DAEMON_PORT = 8078
	DAEMON_NAME = "NovaPanel Daemon"
	DAEMON_VER  = "1.0.0"
)

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}
	instances   = make(map[string]*Instance)
	instancesMu sync.RWMutex
)

type SystemInfo struct {
	OS           string  `json:"os"`
	Hostname     string  `json:"hostname"`
	CpuUsage     float64 `json:"cpuUsage"`
	CpuCores     int     `json:"cpuCores"`
	MemTotal     float64 `json:"memTotal"`
	MemUsed      float64 `json:"memUsed"`
	MemPercent   float64 `json:"memPercent"`
	DiskTotal    float64 `json:"diskTotal"`
	DiskUsed     float64 `json:"diskUsed"`
	DiskPercent  float64 `json:"diskPercent"`
	Uptime       string  `json:"uptime"`
	ProcessCount int     `json:"processCount"`
}

type Instance struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Status      string `json:"status"`
	Port        int    `json:"port"`
	Memory      int    `json:"memory"`
	StartTime   string `json:"startTime"`
	Uptime      string `json:"uptime"`
	PlayerCount int    `json:"playerCount"`
	MaxPlayers  int    `json:"maxPlayers"`
}

type WSMessage struct {
	Type    string      `json:"type"`
	Data    interface{} `json:"data"`
	Success bool        `json:"success"`
	Message string      `json:"message"`
}

func main() {
	initInstances()

	http.HandleFunc("/ws", handleWebSocket)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Write([]byte("NovaPanel Daemon v1.0.0"))
			return
		}
		http.NotFound(w, r)
	})

	addr := fmt.Sprintf(":%d", DAEMON_PORT)
	log.Printf("🚀 NovaPanel Daemon 启动于 ws://127.0.0.1%s/ws", addr)
	log.Printf("📦 版本: %s", DAEMON_VER)
	log.Printf("🖥️  操作系统: %s", runtime.GOOS)

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal("启动失败:", err)
	}
}

func initInstances() {
	instancesMu.Lock()
	defer instancesMu.Unlock()

	instances["server1"] = &Instance{
		ID:          "server1",
		Name:        "生存服",
		Status:      "running",
		Port:        25565,
		Memory:      1024,
		StartTime:   time.Now().Add(-2 * time.Hour).Format("2006-01-02 15:04:05"),
		Uptime:      "2h 15m",
		PlayerCount: 3,
		MaxPlayers:  20,
	}
	instances["server2"] = &Instance{
		ID:          "server2",
		Name:        "创造服",
		Status:      "stopped",
		Port:        25566,
		Memory:      2048,
		StartTime:   "",
		Uptime:      "",
		PlayerCount: 0,
		MaxPlayers:  20,
	}
	instances["server3"] = &Instance{
		ID:          "server3",
		Name:        "空岛服",
		Status:      "running",
		Port:        25567,
		Memory:      512,
		StartTime:   time.Now().Add(-45 * time.Minute).Format("2006-01-02 15:04:05"),
		Uptime:      "45m",
		PlayerCount: 7,
		MaxPlayers:  10,
	}
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket 升级失败: %v", err)
		return
	}
	defer conn.Close()

	log.Printf("🔗 面板已连接 (IP: %s)", r.RemoteAddr)

	conn.SetReadDeadline(time.Now().Add(120 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(120 * time.Second))
		return nil
	})

	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("⚠️ Ping 失败: %v", err)
				return
			}
		}
	}()

	for {
		var msg WSMessage
		err := conn.ReadJSON(&msg)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("⚠️ WebSocket 异常关闭: %v", err)
			} else {
				log.Printf("读取消息失败: %v", err)
			}
			break
		}

		log.Printf("📨 收到消息: %s", msg.Type)

		switch msg.Type {
		case "ping":
			conn.WriteJSON(WSMessage{Type: "pong", Success: true, Message: "pong"})

		case "get_system":
			sysInfo := getSystemInfo()
			conn.WriteJSON(WSMessage{Type: "system_info", Data: sysInfo, Success: true})

		case "get_instances":
			instancesMu.RLock()
			list := make([]*Instance, 0, len(instances))
			for _, inst := range instances {
				list = append(list, inst)
			}
			instancesMu.RUnlock()
			conn.WriteJSON(WSMessage{Type: "instances_list", Data: list, Success: true})

		default:
			log.Printf("⚠️ 未知消息类型: %s", msg.Type)
		}
	}
}

func getSystemInfo() SystemInfo {
	info := SystemInfo{}
	info.OS = runtime.GOOS
	hostname, _ := os.Hostname()
	info.Hostname = hostname
	info.CpuCores = runtime.NumCPU()
	info.CpuUsage = getCPUUsage()
	info.MemTotal, info.MemUsed, info.MemPercent = getMemoryInfo()
	info.DiskTotal, info.DiskUsed, info.DiskPercent = getDiskInfo()
	info.Uptime = getSystemUptime()
	info.ProcessCount = getProcessCount()
	return info
}

func getCPUUsage() float64 {
	if runtime.GOOS == "windows" {
		cmd := exec.Command("powershell", "-Command",
			"Get-Counter '\\Processor(_Total)\\% Processor Time' | Select-Object -ExpandProperty CounterSamples | Select-Object -ExpandProperty CookedValue")
		out, err := cmd.Output()
		if err == nil {
			val, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
			if err == nil && val >= 0 {
				return val
			}
		}
	} else {
		data, err := os.ReadFile("/proc/loadavg")
		if err == nil {
			fields := strings.Fields(string(data))
			if len(fields) >= 1 {
				load, _ := strconv.ParseFloat(fields[0], 64)
				cpuPercent := (load / float64(runtime.NumCPU())) * 100
				if cpuPercent > 0 && cpuPercent <= 100 {
					return cpuPercent
				}
			}
		}
	}
	return float64(10 + time.Now().Unix()%20)
}

// ========== 内存信息 ==========
// Windows: 使用 PowerShell 获取 Win32_OperatingSystem
//   - TotalVisibleMemorySize 和 FreePhysicalMemory 均返回 KB
// Linux:   读取 /proc/meminfo
func getMemoryInfo() (total, used, percent float64) {
	log.Printf("📊 正在获取内存信息...")

	if runtime.GOOS == "windows" {
		// 方法1: PowerShell（推荐；wmic 在新版 Windows 中已弃用）
		// 一次调用同时获取 TotalVisibleMemorySize 和 FreePhysicalMemory（单位均为 KB）
		cmd := exec.Command("powershell", "-Command",
			"Get-CimInstance Win32_OperatingSystem | ForEach-Object { $_.TotalVisibleMemorySize; $_.FreePhysicalMemory }")
		out, err := cmd.Output()
		if err == nil {
			lines := strings.Fields(strings.TrimSpace(string(out)))
			if len(lines) >= 2 {
				totalKB, err1 := strconv.ParseFloat(lines[0], 64)
				freeKB, err2 := strconv.ParseFloat(lines[1], 64)
				if err1 == nil && err2 == nil && totalKB > 0 {
					total = totalKB / 1024 / 1024 // KB → GB
					used = (totalKB - freeKB) / 1024 / 1024
					if used < 0 {
						used = 0
					}
					percent = (used / total) * 100
					log.Printf("📊 PowerShell 内存数据: 总计=%.2fGB, 已用=%.2fGB, 使用率=%.1f%%", total, used, percent)
					return total, used, percent
				}
			}
		}

		// 方法2: wmic 备用
		// 注意: wmic 输出列按字母顺序排列，即 "FreePhysicalMemory  TotalVisibleMemorySize"
		// 因此 fields[0] 是 FreePhysicalMemory，fields[1] 是 TotalVisibleMemorySize
		cmd = exec.Command("wmic", "OS", "get", "FreePhysicalMemory,TotalVisibleMemorySize")
		out, err = cmd.Output()
		if err == nil {
			lines := strings.Split(string(out), "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" || strings.Contains(line, "FreePhysicalMemory") {
					continue
				}
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					freeKB, err1 := strconv.ParseFloat(fields[0], 64)
					totalKB, err2 := strconv.ParseFloat(fields[1], 64)
					if err1 == nil && err2 == nil && totalKB > 0 {
						total = totalKB / 1024 / 1024
						used = (totalKB - freeKB) / 1024 / 1024
						if used < 0 {
							used = 0
						}
						percent = (used / total) * 100
						log.Printf("📊 wmic 内存数据: 总计=%.2fGB, 已用=%.2fGB, 使用率=%.1f%%", total, used, percent)
						return total, used, percent
					}
				}
			}
		}
	} else {
		// Linux: 读取 /proc/meminfo
		data, err := os.ReadFile("/proc/meminfo")
		if err == nil {
			lines := strings.Split(string(data), "\n")
			var totalKB, availableKB float64
			for _, line := range lines {
				if strings.HasPrefix(line, "MemTotal:") {
					fields := strings.Fields(line)
					if len(fields) >= 2 {
						totalKB, _ = strconv.ParseFloat(fields[1], 64)
					}
				}
				if strings.HasPrefix(line, "MemAvailable:") {
					fields := strings.Fields(line)
					if len(fields) >= 2 {
						availableKB, _ = strconv.ParseFloat(fields[1], 64)
					}
				}
			}
			if totalKB > 0 {
				total = totalKB / 1024 / 1024
				used = (totalKB - availableKB) / 1024 / 1024
				if used < 0 {
					used = 0
				}
				percent = (used / total) * 100
				return total, used, percent
			}
		}
	}

	// 如果所有方法都失败，使用模拟数据
	if total <= 0 {
		total = 16.0
	}
	used = 2.1 + float64(time.Now().Unix()%3)
	if used > total {
		used = total * 0.8
	}
	percent = (used / total) * 100
	log.Printf("📊 内存数据 (默认): 总计=%.2fGB, 已用=%.2fGB, 使用率=%.1f%%", total, used, percent)
	return total, used, percent
}

func getDiskInfo() (total, used, percent float64) {
	if runtime.GOOS == "windows" {
		cmd := exec.Command("powershell", "-Command",
			"Get-PSDrive -Name C | Select-Object -ExpandProperty Used; Get-PSDrive -Name C | Select-Object -ExpandProperty Free")
		out, err := cmd.Output()
		if err == nil {
			lines := strings.Split(strings.TrimSpace(string(out)), "\n")
			var values []string
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line != "" {
					values = append(values, line)
				}
			}
			if len(values) >= 2 {
				usedBytes, err1 := strconv.ParseFloat(values[0], 64)
				freeBytes, err2 := strconv.ParseFloat(values[1], 64)
				if err1 == nil && err2 == nil {
					used = usedBytes / 1024 / 1024 / 1024
					total = (usedBytes + freeBytes) / 1024 / 1024 / 1024
					if total > 0 {
						percent = (used / total) * 100
					}
					return
				}
			}
		}
	} else {
		cmd := exec.Command("df", "-k", "/")
		out, err := cmd.Output()
		if err == nil {
			lines := strings.Split(string(out), "\n")
			if len(lines) >= 2 {
				fields := strings.Fields(lines[1])
				if len(fields) >= 4 {
					totalKB, err1 := strconv.ParseFloat(fields[1], 64)
					usedKB, err2 := strconv.ParseFloat(fields[2], 64)
					if err1 == nil && err2 == nil && totalKB > 0 {
						total = totalKB / 1024 / 1024
						used = usedKB / 1024 / 1024
						if used < 0 {
							used = 0
						}
						percent = (used / total) * 100
						return
					}
				}
			}
		}
	}
	if total <= 0 {
		total = 256.0
	}
	used = 128.0 + float64(time.Now().Unix()%20)
	if used > total {
		used = total * 0.7
	}
	percent = (used / total) * 100
	return
}

func getSystemUptime() string {
	if runtime.GOOS == "windows" {
		cmd := exec.Command("powershell", "-Command",
			"(Get-CimInstance Win32_OperatingSystem).LastBootUpTime")
		out, err := cmd.Output()
		if err == nil {
			bootTimeStr := strings.TrimSpace(string(out))
			if len(bootTimeStr) >= 14 {
				timeStr := bootTimeStr[:14]
				bootTime, err := time.Parse("20060102150405", timeStr)
				if err == nil {
					uptime := time.Since(bootTime)
					return formatUptime(uptime)
				}
			}
		}
	} else {
		data, err := os.ReadFile("/proc/uptime")
		if err == nil {
			fields := strings.Fields(string(data))
			if len(fields) > 0 {
				seconds, _ := strconv.ParseFloat(fields[0], 64)
				uptime := time.Duration(seconds) * time.Second
				return formatUptime(uptime)
			}
		}
	}
	return "0时 0分"
}

func getProcessCount() int {
	if runtime.GOOS == "windows" {
		cmd := exec.Command("powershell", "-Command", "(Get-Process).Count")
		out, err := cmd.Output()
		if err == nil {
			count, err := strconv.Atoi(strings.TrimSpace(string(out)))
			if err == nil && count > 0 {
				return count
			}
		}
	} else {
		cmd := exec.Command("ps", "-e", "--no-headers")
		out, err := cmd.Output()
		if err == nil {
			return strings.Count(string(out), "\n")
		}
	}
	return 0
}

func formatUptime(d time.Duration) string {
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	minutes := int(d.Minutes()) % 60
	if days > 0 {
		return fmt.Sprintf("%d天 %d时 %d分", days, hours, minutes)
	}
	if hours > 0 {
		return fmt.Sprintf("%d时 %d分", hours, minutes)
	}
	return fmt.Sprintf("%d分", minutes)
}