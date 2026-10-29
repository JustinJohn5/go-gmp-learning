package main

import "fmt"

// G 代表一个 Goroutine：用户态的任务单元。
// 在真实 runtime 里它还保存栈、状态、pc 等，这里先抽象成一个函数。
type G struct {
	id   int          // goroutine 编号，方便观察
	fn   func()        // 这个 G 要执行的任务
	next *G           // 链表指针，后面 P 的本地队列会用（这一步暂不用）
}

// 状态机：真实 runtime 有 _Gidle/_Grunnable/_Grunning/_Gsyscall/_Gwaiting/_Gdead 等。
// 教学版只关心两个状态：可运行 / 已完成。
const (
	statusRunnable = 1
	statusDone     = 2
)

func newG(id int, fn func()) *G {
	return &G{id: id, fn: fn}
}

// run 执行这个 G 的任务。
// 注意：我们这里是"同步"调用，下一节交给 M 去异步跑。
func (g *G) run() {
	fmt.Printf("  [G%d] 开始执行\n", g.id)
	g.fn()
	fmt.Printf("  [G%d] 执行完成\n", g.id)
}

func main() {
	fmt.Println("=== Step 1: G (Goroutine) ===")

	// 构造三个 G，每个就是一个函数
	g1 := newG(1, func() { fmt.Println("    hello from G1") })
	g2 := newG(2, func() { fmt.Println("    hello from G2") })
	g3 := newG(3, func() { fmt.Println("    hello from G3") })

	// 现在我们自己当"调度器"，依次把它们跑掉
	queue := []*G{g1, g2, g3}
	for _, g := range queue {
		g.run()
	}

	fmt.Println("=== 所有 G 执行完毕 ===")
}
