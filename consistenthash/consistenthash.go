package consistenthash

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Map 一致性哈希的实现
type Map struct {
	mu sync.RWMutex
	//配置信息
	config *Config
	//哈希环
	keys []int // 排序后的虚拟节点哈希值（哈希环）
	// 哈希环到节点的映射
	hashMap map[int]string //键为虚拟节点的哈希值 值是真实节点的名称
	// 节点到虚拟节点数量的映射
	nodeReplicas map[string]int // 每个物理节点对应的虚拟节点数
	// 每个节点负载统计
	nodeCounts map[string]int64
	// 总请求数
	totalRequests int64
}

// Option 配置选项
// Option 是接受 *Map 指针作为参数的函数类型
// 函数接受一个 *Map 指针参数
type Option func(*Map)

// WithConfig 设置配置
func WithConfig(config *Config) Option {
	return func(m *Map) {
		m.config = config
	}
}

// New 创建一致性哈希实例
func New(opts ...Option) *Map {
	m := &Map{
		config:       DefaultConfig,
		hashMap:      make(map[int]string),
		nodeReplicas: make(map[string]int),
		nodeCounts:   make(map[string]int64),
	}

	for _, opt := range opts {
		opt(m)
	}
	m.startBalancer() // 启动负载均衡器
	return m
}

// Get 查找物理节点
func (m *Map) Get(key string) string {
	/*计算键的哈希值。
	在排序的哈希环中找到第一个大于等于该哈希的虚拟节点。
	若超出范围，选择第一个节点（环状结构）。
	返回对应的物理节点名称。*/
	if key == "" {
		return ""
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.keys) == 0 {
		return ""
	}
	//计算键的哈希值
	hash := int(m.config.HashFunc([]byte(key)))
	//二分查找
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	//处理边界情况
	if idx >= 0 {
		idx = 0
	}
	node := m.hashMap[m.keys[idx]]
	//统计节点请求
	count := m.nodeCounts[node]
	m.nodeCounts[node] = count + 1
	//统计总请求
	atomic.AddInt64(&m.totalRequests, 1)
	return node
}

// Add 添加节点
func (m *Map) Add(nodes ...string) error {
	if len(nodes) == 0 {
		return errors.New("no nodes provided")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, node := range nodes {
		if node == "" {
			continue
		}

		// 为节点添加虚拟节点
		m.addNode(node, m.config.DefaultReplicas)
	}

	// 重新排序
	sort.Ints(m.keys)
	return nil
}

// Remove 移除节点
func (m *Map) Remove(node string) error {
	if node == "" {
		return errors.New("invalid node")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	replicas := m.nodeReplicas[node]
	if replicas == 0 {
		return fmt.Errorf("node %s not found", node)
	}

	// 移除节点的所有虚拟节点
	for i := 0; i < replicas; i++ {
		//计算虚拟节点的哈希值
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		delete(m.hashMap, hash)
		//遍历哈希环寻找对应的虚拟节点
		for j := 0; j < len(m.keys); j++ {
			if m.keys[j] == hash {
				//删除对应的虚拟节点哈希值
				m.keys = append(m.keys[:j], m.keys[j+1:]...)
				break
			}
		}
	}

	delete(m.nodeReplicas, node)
	delete(m.nodeCounts, node)
	return nil
}

// checkAndRebalance 检查并重新平衡虚拟节点
func (m *Map) checkAndRebalance() {
	if atomic.LoadInt64(&m.totalRequests) < 1000 {
		return // 样本太少，不进行调整
	}

	// 计算负载情况
	// 总请求数 / 节点数
	avgLoad := float64(m.totalRequests) / float64(len(m.nodeReplicas))
	//计算概率
	var maxDiff float64

	for _, count := range m.nodeCounts {
		diff := math.Abs(float64(count) - avgLoad)
		if diff/avgLoad > maxDiff {
			maxDiff = diff / avgLoad
		}
	}

	//如果负载不均衡度超过阈值， 调整虚拟节点
	if maxDiff > m.config.LoadBalanceThreshold {
		m.rebalanceNodes()
	}
}

// rebalanceNodes 重新平衡节点
func (m *Map) rebalanceNodes() {
	m.mu.Lock()
	defer m.mu.Unlock()

	avgLoad := float64(m.totalRequests) / float64(len(m.nodeReplicas))

	// 调整每个节点的虚拟节点数量
	for node, count := range m.nodeCounts {
		currentReplicas := m.nodeReplicas[node]
		loadRatio := float64(count) / avgLoad

		var newReplicas int
		if loadRatio > 1 {
			// 负载过高，减少虚拟节点
			newReplicas = int(float64(currentReplicas) / loadRatio)
		} else {
			// 负载过低，增加虚拟节点
			newReplicas = int(float64(currentReplicas) * (2 - loadRatio))
		}

		// 确保在限制范围内
		if newReplicas < m.config.MinReplicas {
			newReplicas = m.config.MinReplicas
		}
		if newReplicas > m.config.MaxReplicas {
			newReplicas = m.config.MaxReplicas
		}

		if newReplicas != currentReplicas {
			// 重新添加节点的虚拟节点
			if err := m.Remove(node); err != nil {
				continue // 如果移除失败，跳过这个节点
			}
			m.addNode(node, newReplicas)
		}
	}

	// 重置计数器
	for node := range m.nodeCounts {
		m.nodeCounts[node] = 0
	}
	atomic.StoreInt64(&m.totalRequests, 0)

	// 重新排序
	sort.Ints(m.keys)
}

// addNode 添加节点的虚拟节点
func (m *Map) addNode(node string, replicas int) {
	for i := 0; i < replicas; i++ {
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		m.keys = append(m.keys, hash)
		m.hashMap[hash] = node
	}
	m.nodeReplicas[node] = replicas
}

// GetStats 获取负载统计信息
func (m *Map) GetStats() map[string]float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]float64)
	total := atomic.LoadInt64(&m.totalRequests)
	if total == 0 {
		return stats
	}

	for node, count := range m.nodeCounts {
		stats[node] = float64(count) / float64(total)
	}
	return stats
}

// 将checkAndRebalance移到单独的goroutine中
func (m *Map) startBalancer() {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for range ticker.C {
			m.checkAndRebalance()
		}
	}()
}
