package coroutinekit

import (
	"fmt"
	"github.com/rz1226/serverkit"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

/*
协程管理监控
实现goroutine运行情况监控等功能
适合常驻协程处理任务
不适合随时启动的短时间任务。

m := NewCoroutineKit()
m.Start(  "name", num, f(), panicRestart )


// 如何知道协程退出，把函数包起来
func x(){
	f()
	// 检测退出
}
还有要检测panic，如果panic可能也会退出

*/
const MAXNUM = 100
const STATUSINIT = 0
const STATUSRUN = 1
const STATUSOUT = 2
const STATUSPANIC = 3

var defaultco *CoroutineKit

func init() {
	defaultco = newCoroutineKit()
}

// name=说明  num启动数量  f启动函数  panicRestart异常退出后是否重启  loop正常退出后是否再次调用
func Start(name string, num int, f func(), panicRestart bool, loop bool) {
	defaultco.start(name, num, f, panicRestart, loop)
}
func Show() string {
	return defaultco.showAll()
}

type CoroutineKit struct {
	mu        *sync.Mutex
	nodes     []*Node          // 每一组相同的goroutine占用一个node  主要作用可以按照启动的顺序展示监控信息
	nodeNames map[string]*Node // 保存所有名称，用来去重

}

func newCoroutineKit() *CoroutineKit {
	ck := &CoroutineKit{}
	ck.nodes = make([]*Node, 0, 1000)
	ck.mu = &sync.Mutex{}
	ck.nodeNames = make(map[string]*Node)
	return ck
}

// 加入goroutine，1 名称，不要重复，重复会报错，2 启动多少个goroutine 3 执行函数  4 遇到panic后是否要重新启动 5 loop 是否循环
func (ck *CoroutineKit) start(name string, num int, f func(), panicRestart bool, loop bool) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	name = strings.TrimSpace(name)
	// 检查是否有重复名称
	_, ok := ck.nodeNames[name]
	if ok {
		fmt.Println("coroutinekit start error :duplicated name")
		return
	}
	node := newNode(ck, name, num, f, panicRestart, loop)
	ck.nodes = append(ck.nodes, node)
	ck.nodeNames[name] = node
	node.start() // 启动
}

func (ck *CoroutineKit) showAll() string {
	str := ""

	ck.mu.Lock()
	defer ck.mu.Unlock()
	for _, node := range ck.nodes {
		str += node.showAll()
	}
	return str
}

type Node struct {
	name         string // coroutine名字, 如果没有名字可以填写""
	runnings     []*Routine
	f            func()
	panicRestart bool
	loop         bool
	father       *CoroutineKit
	mu           *sync.Mutex
}

func newNode(father *CoroutineKit, name string, num int, f func(), panicRestart bool, loop bool) *Node {
	if num <= 0 {
		num = 1
	}
	if num > MAXNUM {
		num = MAXNUM
	}
	n := &Node{}
	n.name = name
	n.f = f
	n.panicRestart = panicRestart
	n.loop = loop
	n.father = father
	n.mu = &sync.Mutex{}
	n.runnings = make([]*Routine, num)
	for i := 0; i < num; i++ {
		p := &Routine{}
		p.name = name
		p.startTime = ""
		p.endTime = ""
		p.panicTime = ""
		p.status = STATUSINIT
		p.panicTimes = 0
		p.mu = &sync.Mutex{}
		p.lastPanicInfo = ""
		n.runnings[i] = p
	}
	return n
}

func (n *Node) showAll() string {
	str := ""
	str1 := "正在运行的数量  :"
	str2 := "已经退出的数量  :"
	str3 := "已经panic的数量 :"
	str4 := "总数量          :"
	str5 := "panic历史数量   :"
	count1 := 0
	count2 := 0
	count3 := 0
	count4 := 0
	count5 := 0
	n.mu.Lock()
	defer n.mu.Unlock()
	for k, v := range n.runnings {
		str += "------->\nGoroutine序号：" + strconv.Itoa(k)
		readme, num1, num2, num3, num4, num5 := v.show()
		str += readme
		count1 += num1
		count2 += num2
		count3 += num3
		count4 += num4
		count5 += num5

	}
	return "------------------协程名称：" + n.name + "------数量：" + fmt.Sprint(count4) + "---------------------->>\n" +
		str4 + strconv.Itoa(count4) + "\n" +
		str1 + strconv.Itoa(count1) + "\n" +
		str2 + strconv.Itoa(count2) + "\n" +
		str3 + strconv.Itoa(count3) + "\n" +
		str5 + strconv.Itoa(count5) + "\n" +
		str
}

func (n *Node) start() {
	n.mu.Lock()
	defer n.mu.Unlock()
	num := len(n.runnings)
	for i := 0; i < num; i++ {
		n.startOne(i)
	}
}
func (n *Node) startOne(goroutineNo int) {
	newf := func(no int) {
		defer func() {
			if co := recover(); co != nil {
				// 检查panic
				str := fmt.Sprintln(co)
				strStackInfo := GetPrintStack()
				n.setPanic(no, str+strStackInfo)
			}
		}()
		// 开始运行
		n.setRun(no)
		n.f()
		// 检测退出
		n.setOut(no)
	}
	go newf(goroutineNo)
}

const PANICSLEEPMILLTIME = 100
const LOOPSLEEPMILLTIME = 100

// 发生panic的时候
func (n *Node) setPanic(no int, info string) {
	p := n.runnings[no]
	p.mu.Lock()
	defer p.mu.Unlock()
	atomic.AddUint64(&p.panicTimes, 1) // 原子操作貌似是没有必要的
	p.lastPanicInfo = info
	p.status = STATUSPANIC
	p.panicTime = time.Now().Format("2006-01-02 15:04:05")
	if n.panicRestart {
		time.Sleep(time.Millisecond * PANICSLEEPMILLTIME)
		n.startOne(no)
	}

}

// 正常退出
func (n *Node) setOut(no int) {
	p := n.runnings[no]
	p.mu.Lock()
	defer p.mu.Unlock()
	p.status = STATUSOUT
	p.endTime = time.Now().Format("2006-01-02 15:04:05")
	if n.loop {
		time.Sleep(time.Millisecond * LOOPSLEEPMILLTIME)
		n.startOne(no)
	}
}

// 开始运行
func (n *Node) setRun(no int) {
	p := n.runnings[no]
	p.mu.Lock()
	defer p.mu.Unlock()
	p.status = STATUSRUN
	p.startTime = time.Now().Format("2006-01-02 15:04:05")
	p.endTime = ""
}

type Routine struct {
	mu            *sync.Mutex
	name          string
	startTime     string
	endTime       string
	panicTime     string
	status        uint32 // 0没有启动 1运行中 2退出 3panic
	panicTimes    uint64 // panic发生的次数
	lastPanicInfo string // 最后一次panic的信息
}

// string信息收集  num1启动中的数量 num2已经退出的数量 num3已经panic的数量 num4 总数量  num5 历史panic数量
func (r *Routine) show() (readme string, countRun, countQuit, countPanic, countAll, countHistory int) {
	str := ""
	num1 := 0
	num2 := 0
	num3 := 0
	num4 := 0
	num5 := 0
	r.mu.Lock()
	defer r.mu.Unlock()
	str += "\nGoroutine名称:" + r.name + "\n"
	statusReadme := ""
	if r.status == STATUSINIT {
		statusReadme = "未启动"
	} else if r.status == STATUSRUN {
		statusReadme = "运行中"
		num1 = 1
	} else if r.status == STATUSOUT {
		statusReadme = "已退出"
		num2 = 1
	} else if r.status == STATUSPANIC {
		statusReadme = "已恐慌"
		num3 = 1
	}
	num4 = 1
	num5 = int(r.panicTimes)
	str += "状态     :" + statusReadme + "\n"
	str += "启动时间  :" + r.startTime + "\n"
	str += "退出时间  :" + r.endTime + "\n"
	str += "异常时间  :" + r.panicTime + "\n"
	str += "异常次数  :" + strconv.FormatUint(r.panicTimes, 10) + "\n"
	str += "最后异常信息:" + r.lastPanicInfo + "\n"

	return str, num1, num2, num3, num4, num5
}

/*



 *********************************************监控**************************************************


 */

var StartedMonitor int32 = 0

func StartMonitor(port string) {
	if atomic.CompareAndSwapInt32(&StartedMonitor, 0, 1) {
		go serverkit.NewSimpleHTTPServer().Add("/", httpShowAll).Start(port)
	}

}

func httpShowAll(w http.ResponseWriter, r *http.Request) {
	str := defaultco.showAll()
	fmt.Fprintln(w, str)
}

func GetPrintStack() string {
	buf := debug.Stack()
	return fmt.Sprintf("==> %s\n", string(buf))
}
