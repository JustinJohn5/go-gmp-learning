package main

import (
	"fmt"
	"sync"
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
	id     int
	mu     sync.Mutex
	runq   []*G
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

// === M ===
// M 是 OS 线程的抽象。它必须绑定一个 P 才能跑 G。
type M struct {
	id      int
	p       *P     // 当前绑定的 P（运行时也叫 m.p）
	curg    *G     // 当前正在跑的 G（m.curg）
	stopCh  chan struct{}
}

func newM(id int) *M {
	return &M{id: id, stopCh: make(chan struct{})}
}

// bindP 绑定一个 P。M 一旦绑了 P，才有"通行证"去 P 的本地队列里取 G。
func (m *M) bindP(p *P) {
	fmt.Printf("  [M%d] 绑定 %s\n", m.id, p)
	m.p = p
}

// schedule 是 M 的核心调度循环（简化版）：
//   1. 从绑定的 P 的本地队列 pop 一个 G
//   2. pop 到了 → 跑它；pop 不到 → M 退出调度循环（这一步先这样）
// 真实 runtime：队列空了会去全局队列找 → 还空就 work stealing → 还空就 park M。
func (m *M) schedule() {
	for {
		select {
		case <-m.stopCh:
			fmt.Printf("  [M%d] 收到停止信号，退出\n", m.id)
			return
		default:
		}
		g := m.p.pop()
		if g == nil {
			// 本地队列空了。Step3 先简单退出；Step5 改成去偷别人的。
			fmt.Printf("  [M%d] %s 队列空了，park（休眠）\n", m.id, m.p)
			return
		}
		m.curg = g
		fmt.Printf("  [M%d] 拿到 %s 的 G%d，开始执行\n", m.id, m.p, g.id)
		g.fn() // 真实 runtime 这里是 gogo() 切换栈，切换到 G 的栈上跑
		m.curg = nil
	}
}

func (p *P) String() string { return fmt.Sprintf("P%d", p.id) }

func main() {
	fmt.Println("=== Step 3: M 绑定 P 跑 G ===")

	p0 := newP(0)
	p1 := newP(1)

	// 造 5 个 G，分给两个 P
	p0.push(newG(1, func() { fmt.Println("    G1 running") }))
	p0.push(newG(2, func() { fmt.Println("    G2 running") }))
	p0.push(newG(3, func() { fmt.Println("    G3 running") }))
	p1.push(newG(4, func() { fmt.Println("    G4 running") }))
	p1.push(newG(5, func() { fmt.Println("    G5 running") }))

	// 起两个 M，分别绑 P0、P1，并发跑
	// 注意：我们用 Go 的 go 关键字来"模拟"OS 线程（M）。
	var wg sync.WaitGroup
	for _, pair := range []struct{ m *M; p *P }{{newM(0), p0}, {newM(1), p1}} {
		wg.Add(1)
		go func(m *M, p *P) {
			defer wg.Done()
			m.bindP(p)
			m.schedule()
		}(pair.m, pair.p)
	}
	wg.Wait()

	fmt.Println("=== Step 3 完成（注意：两个 M 是并发跑的）===")
	_ = time.Second
}
