package main

import (
	"fmt"
	"sync"
)

// === G === （沿用 Step1，简化掉不需要的字段）
type G struct {
	id int
	fn func()
}

func newG(id int, fn func()) *G { return &G{id: id, fn: fn} }

func (g *G) run() {
	fmt.Printf("    [G%d] 跑起来了\n", g.id)
	g.fn()
}

// === P ===
// P 是"逻辑处理器"：它持有一个本地运行队列。
// M 必须先"绑定"一个 P，才能从这个队列里拿 G 来跑。
// P 的数量 = 同时可执行 G 的并行度，等价于 GOMAXPROCS。

const (
	statusPidle     = 0 // 空闲：没人绑定
	statusPrunning  = 1 // 已被 M 绑定，正在跑
	statusPsyscall  = 2 // 绑定的 M 陷入了系统调用（Step6 才用）
)

type P struct {
	id      int
	status  int
	mu      sync.Mutex      // 保护 runq（真实 runtime 是无锁的环形数组）
	runq    []*G            // 本地运行队列（FIFO，简化版）
}

func newP(id int) *P {
	return &P{id: id, status: statusPidle, runq: make([]*G, 0, 256)}
}

// push 把 G 放入 P 的本地队列。
// 真实 runtime 里，go func() 时会把新 G push 到当前 P 的 runq。
func (p *P) push(g *G) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.runq = append(p.runq, g)
}

// pop 从本地队列头取一个 G。队列为空返回 nil（nil 意味着"我要去别处找活干"）。
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

func (p *P) queueLen() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.runq)
}

func main() {
	fmt.Println("=== Step 2: P (Processor) + 本地运行队列 ===")

	// 造两个 P，模拟 GOMAXPROCS=2
	p0 := newP(0)
	p1 := newP(1)

	// 把 G 散到不同 P 的本地队列（真实 runtime 会做负载均衡，这里手动分）
	p0.push(newG(1, func() { fmt.Println("    G1 from P0") }))
	p0.push(newG(2, func() { fmt.Println("    G2 from P0") }))
	p1.push(newG(3, func() { fmt.Println("    G3 from P1") }))

	fmt.Printf("P0 队列长度=%d, P1 队列长度=%d\n", p0.queueLen(), p1.queueLen())

	// 现在仍然由 main 当调度器：挨个从 P 的本地队列 pop 出 G 跑掉
	// 注意：这里还没有 M，是直接消费 P。Step3 才会让 M 上场。
	for _, p := range []*P{p0, p1} {
		fmt.Printf("--- 消费 %s 的本地队列 ---\n", p)
		for {
			g := p.pop()
			if g == nil {
				break
			}
			g.run()
		}
	}

	fmt.Println("=== Step 2 完成 ===")
}

func (p *P) String() string { return fmt.Sprintf("P%d", p.id) }
