package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// === G ===
// 注意 fn 的签名变了：现在接收一个 *runtime，G 可以通过它调用 Syscall 等"系统调用"。
type G struct {
	id int
	fn func(*runtime)
}

func newG(id int, fn func(*runtime)) *G { return &G{id: id, fn: fn} }

// === P ===
type P struct {
	id   int
	mu   sync.Mutex
	runq []*G
}

func newP(id int) *P { return &P{id: id, runq: make([]*G, 0, 256)} }
func (p *P) push(g *G)            { p.mu.Lock(); defer p.mu.Unlock(); p.runq = append(p.runq, g) }
func (p *P) pop() *G              { p.mu.Lock(); defer p.mu.Unlock(); if len(p.runq) == 0 { return nil }; g := p.runq[0]; p.runq = p.runq[1:]; return g }
func (p *P) len() int             { p.mu.Lock(); defer p.mu.Unlock(); return len(p.runq) }
func (p *P) stealHalf() []*G {
	p.mu.Lock(); defer p.mu.Unlock()
	n := len(p.runq) / 2
	if n == 0 { return nil }
	half := make([]*G, n); copy(half, p.runq[len(p.runq)-n:]); p.runq = p.runq[:len(p.runq)-n]
	return half
}
func (p *P) String() string { return fmt.Sprintf("P%d", p.id) }

// === M ===
type M struct {
	id  int
	p   *P
	curg *G
}
func newM(id int) *M { return &M{id: id} }

// === runtime：G 执行时拿到的"运行时上下文" ===
// 类似真实 runtime 里的 getg()+gettls()，提供 G 主动调用运行时服务的入口。
type runtime struct {
	sched *Scheduler
	m     *M
	p     *P
	g     *G
}

// Syscall 模拟一次阻塞系统调用。
// 流程：
//  1. hand-off：解绑当前 P，P 回 idle 池，唤醒别的 M 来接手
//  2. 阻塞执行 block()（此时本 M 没绑 P，卡死也不影响别人）
//  3. 重新获取一个 P（拿不到就自旋等——简化；真实 runtime 会 park M + 把 G 推回全局队列）
func (r *runtime) Syscall(name string, block func()) {
	fmt.Printf("    [G%d] 进 syscall %q，handoff %s\n", r.g.id, name, r.p)
	// 1. 解绑 P
	oldP := r.p
	r.sched.releaseP(oldP)
	r.p = nil
	r.m.p = nil
	// 2. 阻塞
	block()
	// 3. 重新获取 P
	newP := r.sched.acquireP()
	r.p = newP
	r.m.p = newP
	fmt.Printf("    [G%d] syscall %q 返回，重新绑 %s\n", r.g.id, name, newP)
}

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

func (s *Scheduler) SpawnG(fn func(*runtime)) {
	gid := atomic.AddInt64(&s.gid, 1)
	g := newG(int(gid), fn)
	s.wg.Add(1)

	s.mu.Lock()
	if len(s.idlep) > 0 {
		p := s.idlep[len(s.idlep)-1]
		s.idlep = s.idlep[:len(s.idlep)-1]
		mid := len(s.allm)
		m := newM(mid)
		s.allm = append(s.allm, m)
		s.mu.Unlock()
		p.push(g)
		go s.mStart(m, p)
		return
	}
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

func (s *Scheduler) schedule(m *M, p *P) {
	for {
		if g := p.pop(); g != nil { s.runG(m, p, g); continue }
		if batch := s.grabGlobal(); batch != nil { for _, g := range batch { p.push(g) }; continue }
		if stolen := s.stealFromOthers(p); stolen != nil { for _, g := range stolen { p.push(g) }; continue }
		if batch := s.grabGlobal(); batch != nil { for _, g := range batch { p.push(g) }; continue }
		return
	}
}

func (s *Scheduler) runG(m *M, p *P, g *G) {
	m.curg = g
	fmt.Printf("  [M%d@%s] 跑 G%d\n", m.id, p, g.id)
	// 给 G 一个运行时上下文，里面持有 m/p/g，G 通过它调用 Syscall 等
	r := &runtime{sched: s, m: m, p: p, g: g}
	g.fn(r)
	// G 跑完了，恢复一下（如果中间发生过 handoff，r.p 可能已经变了，但 m.p 是当前真实绑定）
	m.curg = nil
	s.wg.Done()
}

func (s *Scheduler) grabGlobal() []*G {
	s.mu.Lock(); defer s.mu.Unlock()
	if len(s.globalq) == 0 { return nil }
	batch := s.globalq; s.globalq = nil; return batch
}

func (s *Scheduler) stealFromOthers(myP *P) []*G {
	for _, other := range s.allp {
		if other == myP || other.len() == 0 { continue }
		if half := other.stealHalf(); half != nil {
			fmt.Printf("  [work-steal] %s 从 %s 偷了 %d 个 G\n", myP, other, len(half))
			return half
		}
	}
	return nil
}

// releaseP 把 P 还回 idle 池（hand-off 的关键：让别的 M 能接手）。
func (s *Scheduler) releaseP(p *P) {
	s.mu.Lock()
	s.idlep = append(s.idlep, p)
	hasGlobalWork := len(s.globalq) > 0
	s.mu.Unlock()
	// 如果全局队列还有 G，唤醒一个新 M 来消费（接手这个 P）
	if hasGlobalWork {
		s.wakeMFromIdle()
	}
	// 即使没全局队列的 G，别的 M 在 work stealing 失败后也会来 idle 池找 P
}

// wakeMFromIdle：从 idle 池借一个 P，起一个 M 跑调度循环（消费全局队列）。
func (s *Scheduler) wakeMFromIdle() {
	s.mu.Lock()
	if len(s.idlep) == 0 { s.mu.Unlock(); return }
	p := s.idlep[len(s.idlep)-1]
	s.idlep = s.idlep[:len(s.idlep)-1]
	mid := len(s.allm)
	m := newM(mid)
	s.allm = append(s.allm, m)
	s.mu.Unlock()
	go s.mStart(m, p)
}

// acquireP 阻塞等待直到能从 idle 池拿到一个 P。
// 真实 runtime：拿不到就把 G push 到全局队列，M park。这里简化为忙等。
func (s *Scheduler) acquireP() *P {
	for {
		s.mu.Lock()
		if len(s.idlep) > 0 {
			p := s.idlep[len(s.idlep)-1]
			s.idlep = s.idlep[:len(s.idlep)-1]
			s.mu.Unlock()
			return p
		}
		s.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
}

func main() {
	fmt.Println("=== Step 6: 阻塞 syscall 与 P hand-off ===")
	rand.Seed(time.Now().UnixNano())

	s := NewScheduler(2) // 只有 2 个 P，演示 P 紧张时 hand-off

	// SpawnG 4 个 G，其中 2 个会进 syscall 阻塞
	for i := 0; i < 4; i++ {
		idx := i
		s.SpawnG(func(r *runtime) {
			fmt.Printf("    G%d 开始\n", idx+1)
			// 一半概率进"syscall"阻塞 50ms
			if idx%2 == 0 {
				r.Syscall(fmt.Sprintf("read#%d", idx+1), func() {
					fmt.Printf("    G%d 正在阻塞读 IO...\n", idx+1)
					time.Sleep(50 * time.Millisecond)
				})
			}
			fmt.Printf("    G%d 结束\n", idx+1)
		})
	}

	s.wg.Wait()
	fmt.Println("=== Step 6 完成 ===")
	fmt.Println("注意：G 进 syscall 时把 P 让出来，别的 G 得以继续跑；syscall 返回后再拿回 P。")
}
