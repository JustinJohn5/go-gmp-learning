package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// === G ===
type G struct {
	id int
	fn func()
}

func newG(id int, fn func()) *G { return &G{id: id, fn: fn} }

// === P ===
type P struct {
	id   int
	mu   sync.Mutex
	runq []*G
}

func newP(id int) *P { return &P{id: id, runq: make([]*G, 0, 256)} }

func (p *P) push(g *G) {
	p.mu.Lock(); defer p.mu.Unlock()
	p.runq = append(p.runq, g)
}
func (p *P) pop() *G {
	p.mu.Lock(); defer p.mu.Unlock()
	if len(p.runq) == 0 { return nil }
	g := p.runq[0]; p.runq = p.runq[1:]; return g
}
func (p *P) len() int { p.mu.Lock(); defer p.mu.Unlock(); return len(p.runq) }
func (p *P) String() string { return fmt.Sprintf("P%d", p.id) }

// stealHalf 偷这个 P 本地队列的"一半" G 返回给 caller。
// 真实 runtime：偷 (len-1)/2 个，且用 CAS 无锁操作。这里简化用 mutex。
// 返回 nil 表示这个 P 也没货。
func (p *P) stealHalf() []*G {
	p.mu.Lock(); defer p.mu.Unlock()
	n := len(p.runq) / 2
	if n == 0 { return nil }
	half := make([]*G, n)
	copy(half, p.runq[len(p.runq)-n:]) // 从尾部拿一半（真实 runtime 也是从尾部偷）
	p.runq = p.runq[:len(p.runq)-n]
	return half
}

// === M ===
type M struct {
	id  int
	p   *P
	curg *G
}

func newM(id int) *M { return &M{id: id} }

// === Scheduler ===
type Scheduler struct {
	mu         sync.Mutex
	gomaxprocs int
	allp       []*P
	idlep      []*P
	allm       []*M
	globalq    []*G
	gid        int64
	wg         sync.WaitGroup
}

func NewScheduler(gomaxprocs int) *Scheduler {
	s := &Scheduler{gomaxprocs: gomaxprocs}
	for i := 0; i < gomaxprocs; i++ {
		p := newP(i)
		s.allp = append(s.allp, p)
		s.idlep = append(s.idlep, p)
	}
	return s
}

// SpawnG：优先放到当前 P 的本地队列（教学版没有 tls，简化为：随机挑一个 idle P；
// 没 idle P 就放全局队列）。真实 runtime 是放当前 P 本地队列。
func (s *Scheduler) SpawnG(fn func()) {
	gid := atomic.AddInt64(&s.gid, 1)
	g := newG(int(gid), fn)
	s.wg.Add(1)

	// 找一个 idle P，把 G 直接放它的本地队列，再唤醒一个 M 绑它
	s.mu.Lock()
	var targetP *P
	if len(s.idlep) > 0 {
		targetP = s.idlep[len(s.idlep)-1]
		s.idlep = s.idlep[:len(s.idlep)-1]
	}
	if targetP != nil {
		targetP.push(g)
		mid := len(s.allm)
		m := newM(mid)
		s.allm = append(s.allm, m)
		s.mu.Unlock()
		go s.mStart(m, targetP)
		return
	}
	// 没 idle P，放全局队列（其它 M park 醒来后会查全局队列）
	s.globalq = append(s.globalq, g)
	s.mu.Unlock()
}

func (s *Scheduler) mStart(m *M, p *P) {
	fmt.Printf("  [M%d] 绑定 %s\n", m.id, p)
	m.p = p
	s.schedule(m, p)
	s.mu.Lock()
	s.idlep = append(s.idlep, p)
	fmt.Printf("  [M%d] park，归还 %s\n", m.id, p)
	s.mu.Unlock()
}

// schedule —— Step5 升级版
// 优先级：本地队列 → 全局队列 → work stealing → 全局再查一次 → park
func (s *Scheduler) schedule(m *M, p *P) {
	for {
		// 1. 吃本地
		if g := p.pop(); g != nil {
			s.runG(m, p, g)
			continue
		}
		// 2. 本地空 → 去全局队列捞一批
		if batch := s.grabGlobal(); batch != nil {
			for _, g := range batch { p.push(g) }
			continue
		}
		// 3. 全局也空 → work stealing：遍历别的 P，偷一半
		if stolen := s.stealFromOthers(p); stolen != nil {
			for _, g := range stolen { p.push(g) }
			continue
		}
		// 4. 偷也偷不到 → 再查一次全局（防止竞态：刚才查完又有新 G 入了全局队列）
		if batch := s.grabGlobal(); batch != nil {
			for _, g := range batch { p.push(g) }
			continue
		}
		// 5. 真没活了 → park
		return
	}
}

func (s *Scheduler) runG(m *M, p *P, g *G) {
	m.curg = g
	fmt.Printf("  [M%d@%s] 跑 G%d\n", m.id, p, g.id)
	g.fn()
	m.curg = nil
	s.wg.Done()
}

// grabGlobal 从全局队列拿一批 G（简化：一次性拿光）。
func (s *Scheduler) grabGlobal() []*G {
	s.mu.Lock(); defer s.mu.Unlock()
	if len(s.globalq) == 0 { return nil }
	batch := s.globalq
	s.globalq = nil
	return batch
}

// stealFromOthers 遍历所有 P（除自己），偷第一个有货的 P 的一半 G。
// 真实 runtime 会随机起点 + 多次重试 + 跳过自己，这里简化。
func (s *Scheduler) stealFromOthers(myP *P) []*G {
	for _, other := range s.allp {
		if other == myP { continue }
		if other.len() == 0 { continue }
		if half := other.stealHalf(); half != nil {
			fmt.Printf("  [work-steal] %s 从 %s 偷了 %d 个 G\n", myP, other, len(half))
			return half
		}
	}
	return nil
}

func main() {
	fmt.Println("=== Step 5: Work Stealing ===")

	s := NewScheduler(3) // 3 个 P，并行度=3

	// 先往 P0 塞 9 个 G，让 P0"很忙"，别的 P 空着
	// 这样 work stealing 演示效果明显
	for i := 0; i < 9; i++ {
		idx := i
		s.SpawnG(func() {
			fmt.Printf("    G%d 跑完\n", idx+1)
			time.Sleep(10 * time.Millisecond) // 模拟一点工作量，让偷窃有机会发生
		})
	}

	s.wg.Wait()
	fmt.Println("=== Step 5 完成：注意上面的 [work-steal] 行 ===")
}
