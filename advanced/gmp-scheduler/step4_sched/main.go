package main

import (
	"fmt"
	"sync"
)

// === G ===
type G struct {
	id int
	fn func()
}

func newG(id int, fn func()) *G { return &G{id: id, fn: fn} }

// === P ===
const localRunqCap = 8 // 本地队列容量，故意调小，方便演示"满了搬一半到全局"

type P struct {
	id   int
	mu   sync.Mutex
	runq []*G
}

func newP(id int) *P { return &P{id: id, runq: make([]*G, 0, localRunqCap)} }

func (p *P) push(g *G) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.runq = append(p.runq, g)
}
func (p *P) pop() *G {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.runq) == 0 {
		return nil
	}
	g := p.runq[0]
	p.runq = p.runq[1:]
	return g
}
func (p *P) len() int       { p.mu.Lock(); defer p.mu.Unlock(); return len(p.runq) }
func (p *P) String() string { return fmt.Sprintf("P%d", p.id) }

// runqStealHalf 把本地队列"一半"搬到 caller 提供的切片里。
// 真实 runtime 的 runqputslow/runqgrab：本地队列满时把一半 G 扔进全局队列，
// 既缓解本地压力，又给别的 M 留活干。
func (p *P) drainHalf() []*G {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := len(p.runq) / 2
	if n == 0 {
		return nil
	}
	half := make([]*G, n)
	copy(half, p.runq[:n])
	p.runq = p.runq[n:]
	return half
}

// === M ===
type M struct {
	id   int
	p    *P
	curg *G
}

func newM(id int) *M { return &M{id: id} }

// === Scheduler ===
type Scheduler struct {
	mu         sync.Mutex
	gomaxprocs int
	allp       []*P
	idlep      []*P // 空闲 P 池（M 启动时来这借一个）
	allm       []*M
	globalq    []*G           // 全局运行队列（兜底）
	gid        int            // G 的自增 id
	wg         sync.WaitGroup // 等所有 G 跑完
}

func NewScheduler(gomaxprocs int) *Scheduler {
	s := &Scheduler{gomaxprocs: gomaxprocs}
	for i := 0; i < gomaxprocs; i++ {
		p := newP(i)
		s.allp = append(s.allp, p)
		s.idlep = append(s.idlep, p) // 初始所有 P 都空闲
	}
	return s
}

// SpawnG 是对外暴露的"go func()"。
// 规则：如果有当前 P（在 M 调度循环里），优先放当前 P 本地队列；
// 否则放全局队列。本地队列满了 → 搬一半到全局队列。
// 这里简化：没有"当前 P"概念时一律放全局队列。
func (s *Scheduler) SpawnG(fn func()) {
	s.mu.Lock()
	s.gid++
	g := newG(s.gid, fn)
	// 先尝试放全局队列（Step4 简化版；Step5 会改成优先放当前 P 本地队列）
	s.globalq = append(s.globalq, g)
	s.wg.Add(1)
	s.mu.Unlock()

	// 有空闲 P 就唤醒一个 M 去跑
	s.wakeM()
}

// wakeM：如果有空闲 P 且没有 idle M 在跑，就起一个 M 绑上 P 跑调度循环。
// 真实 runtime 有 M 的空闲池复用；这里每次都 new 一个，简化。
func (s *Scheduler) wakeM() {
	s.mu.Lock()
	if len(s.idlep) == 0 {
		s.mu.Unlock()
		return // 没有 P 可借，等别的 M park 时归还 P
	}
	p := s.idlep[len(s.idlep)-1]
	s.idlep = s.idlep[:len(s.idlep)-1]
	mid := len(s.allm)
	m := newM(mid)
	s.allm = append(s.allm, m)
	s.mu.Unlock()

	go s.mStart(m, p)
}

// mStart 是 M 的入口：绑 P → 进调度循环 → 退出时归还 P。
func (s *Scheduler) mStart(m *M, p *P) {
	fmt.Printf("  [M%d] 绑定 %s，进入调度循环\n", m.id, p)
	m.p = p
	s.schedule(m, p)
	// 调度循环结束（没活干了）：归还 P 到 idle 池
	s.mu.Lock()
	s.idlep = append(s.idlep, p)
	fmt.Printf("  [M%d] park，归还 %s 到 idle 池\n", m.id, p)
	s.mu.Unlock()
}

// schedule 是 M 的核心循环。
// 规则：
//  1. 先吃本地 P 的队列
//  2. 本地空了 → 去全局队列捞一批（一次捞 len(local)/2 + 1，这里简化为全捞）
//  3. 全局也空了 → park（归还 P，退出循环）
func (s *Scheduler) schedule(m *M, p *P) {
	for {
		// 1. 吃本地
		g := p.pop()
		if g == nil {
			// 2. 本地空 → 去全局队列捞
			s.mu.Lock()
			if len(s.globalq) == 0 {
				s.mu.Unlock()
				return // 没活干了，park
			}
			// 一次捞一批（这里简化：全捞光；真实 runtime 是捞一半以保持负载均衡）
			batch := s.globalq
			s.globalq = nil
			s.mu.Unlock()
			for _, gg := range batch {
				p.push(gg)
			}
			continue
		}
		// 3. 跑 G
		m.curg = g
		fmt.Printf("  [M%d@%s] 跑 G%d\n", m.id, p, g.id)
		g.fn()
		m.curg = nil
		s.wg.Done()
	}
}

func main() {
	fmt.Println("=== Step 4: 完整 Scheduler ===")

	s := NewScheduler(2) // GOMAXPROCS=2，2 个 P

	// 一次性 Spawn 10 个 G
	for i := 0; i < 10; i++ {
		idx := i
		s.SpawnG(func() {
			fmt.Printf("    G%d 业务逻辑跑完\n", idx+1)
		})
	}

	s.wg.Wait()
	fmt.Println("=== Step 4 完成：所有 G 都跑完了 ===")
}
