package memtable

import (
	"bytes"
	"fmt"
	"sync"
)

// 注意 b树的实现参考的是 可视化的逻辑进行的
// https://www.cs.usfca.edu/~galles/visualization/BTree.html
// 但可以根据具体的应用场景进行切换
// 对于删除操作 后续进行补充实现
// 目前使用懒删除 标记删除即可
// 已经达到学习要求了
type data struct {
	key     []byte
	value   interface{}
	deleted bool
}

func newData(key []byte, value interface{}) *data {
	return &data{key, value, false}
}
func (i *data) info() string {
	return fmt.Sprintf("(%s:%s:%v)", i.key, i.value, i.deleted)
}

type DataItem []*data

func (dt *DataItem) showInfo() (ans string) {
	for k, v := range *dt {
		ans += fmt.Sprintf("idx:%d:%v", k, v.info()) + " "
	}
	return
}

func (dt DataItem) changeData(index int, item *data) {
	dt[index].value = item.value
	dt[index].deleted = item.deleted
}

func (dt DataItem) search(data *data) (int, bool) {
	left := 0
	right := len(dt) - 1
	ans := -1

	for left <= right {
		mid := (left + right) / 2
		if bytes.Equal(dt[mid].key, data.key) {
			return mid, true
		} else if bytes.Compare(dt[mid].key, data.key) < 0 {
			ans = mid
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return ans + 1, false
}
func setParentBTree(nodes []*btreeNode, parent *btreeNode) {
	for _, v := range nodes {
		v.parent = parent
	}
}

type btreeNode struct {
	entries  DataItem //[]*data
	children []*btreeNode
	parent   *btreeNode
}
type btree struct {
	root  *btreeNode
	order int
	size  int
	mu    *sync.RWMutex // 读写互斥锁
}

// 这部分存在差异 但并不影响
func (bt *btree) maxEntries() int {
	return bt.order - 1
}

func (bt *btree) middle() int {
	return (bt.order - 1) / 2
}
func (bt *btree) isLeaf(node *btreeNode) bool {
	return len(node.children) == 0
}
func (bt *btree) shouldSplit(node *btreeNode) bool {
	return len(node.entries) > bt.maxEntries()
}

func (bt *btree) Put(item *data) {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if bt.root == nil {
		bt.root = &btreeNode{entries: DataItem{item}}
		return
	}
	if bt.insert(bt.root, item) {
		bt.size++
	}
}
func (bt *btree) delete(node *btreeNode, idx int) {
	node.entries[idx].deleted = true
	bt.size--
}
func (bt *btree) Remove(item *data) bool {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	node, idx, ok := bt.search(bt.root, item)
	if ok {
		bt.delete(node, idx)
	}
	return ok
}
func (bt *btree) search(node *btreeNode, item *data) (st *btreeNode, idx int, ok bool) {
	if bt.root == nil {
		return nil, -1, false
	}
	st = node
	for {
		idx, ok = node.entries.search(item)
		if ok {
			return node, idx, true
		}
		if bt.isLeaf(node) {
			return nil, -1, false
		}
		node = node.children[idx]
	}
}
func (bt *btree) Get(item *data) bool {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	node, index, ok := bt.search(bt.root, item)
	if ok && !node.entries[index].deleted {
		item.value = node.entries[index].value
	}
	//对于数据进行查找处理
	return ok && !node.entries[index].deleted
}
func (bt *btree) insertLeaf(node *btreeNode, item *data) bool {
	index, ok := node.entries.search(item)
	if ok {
		// node.entries.changeData(index, item)
		node.entries[index] = item
		return false
	}
	node.entries = append(node.entries, nil)
	copy(node.entries[index+1:], node.entries[index:])
	node.entries[index] = item
	bt.split(node)
	return true
}
func (bt *btree) split(node *btreeNode) {
	if !bt.shouldSplit(node) {
		return
	}
	if bt.root == node {
		bt.splitRoot()
		return
	}
	bt.splitNonRoot(node)
}

func (bt *btree) splitRoot() {
	mid := bt.middle()
	node := bt.root

	midItem := node.entries[mid]
	newRoot := &btreeNode{entries: DataItem{midItem}}

	leftNode := &btreeNode{entries: node.entries[:mid], parent: newRoot}
	// rightNode := &btreeNode{entries: node.entries[mid+1:], parent: newRoot}
	rightNode := &btreeNode{entries: node.entries[mid+1:], parent: newRoot}

	if !bt.isLeaf(node) {
		leftNode.children = node.children[:mid+1]
		rightNode.children = node.children[mid+1:]
		setParentBTree(leftNode.children, leftNode)
		setParentBTree(rightNode.children, rightNode)
	}
	newRoot.children = []*btreeNode{leftNode, rightNode}
	bt.root = newRoot
}

func (bt *btree) splitNonRoot(node *btreeNode) {
	mid := bt.middle()
	parent := node.parent

	midItem := node.entries[mid]

	leftNode := &btreeNode{entries: node.entries[:mid], parent: parent}
	rightNode := &btreeNode{entries: node.entries[mid+1:], parent: parent}
	if !bt.isLeaf(node) {
		leftNode.children = node.children[:mid+1]
		rightNode.children = node.children[mid+1:]
		setParentBTree(leftNode.children, leftNode)
		setParentBTree(rightNode.children, rightNode)
	}
	index, _ := parent.entries.search(midItem)

	parent.entries = append(parent.entries, nil)
	copy(parent.entries[index+1:], parent.entries[index:])
	parent.entries[index] = midItem

	parent.children[index] = leftNode
	parent.children = append(parent.children, nil)
	copy(parent.children[index+2:], parent.children[index+1:])
	parent.children[index+1] = rightNode

	bt.split(parent)
}
func (bt *btree) insertInnner(node *btreeNode, item *data) bool {
	index, ok := node.entries.search(item)
	if ok {
		node.entries.changeData(index, item)
		return false
	}
	return bt.insert(node.children[index], item)
}
func (bt *btree) insert(node *btreeNode, item *data) bool {
	if bt.isLeaf(node) {
		return bt.insertLeaf(node, item)
	}
	return bt.insertInnner(node, item)
}
func NewBTree(order int) *btree {
	return &btree{order: max(order, 3), mu: &sync.RWMutex{}}
}

func (bt *btree) PrintTree() {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	bt.printTree(bt.root, 0, " ")
}
func (bt *btree) printTree(node *btreeNode, depth int, prefix string) {
	if depth == 0 {
		fmt.Printf("+--%s\n", node.entries.showInfo())
		depth++
	}
	childCount := len(node.children)
	for idx, child := range node.children {
		last := idx == childCount-1
		newPrefix := prefix
		if last {
			newPrefix += "   "
		} else {
			newPrefix += " |  "
		}
		fmt.Println(prefix, childPrefix(last), child.entries.showInfo())
		bt.printTree(child, depth+1, newPrefix)
	}
}
func childPrefix(isLastChild bool) string {
	if isLastChild {
		return "└-- "
	}
	return "|-- "
}

// 中序遍历函数
func (t *btree) InOrderTraversal(fn func(*data)) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	t.inOrderTraversalNode(t.root, fn)
}

// 递归中序遍历节点函数
func (t *btree) inOrderTraversalNode(node *btreeNode, fn func(*data)) {
	if node == nil {
		return
	}

	n := len(node.entries)
	for i := 0; i < n; i++ {
		// 先遍历当前 entry 的左子树
		if i < len(node.children) {
			t.inOrderTraversalNode(node.children[i], fn)
		}
		// 处理当前 entry
		fn(node.entries[i])
	}
	// 遍历最后一个 entry 的右子树
	if n < len(node.children) {
		t.inOrderTraversalNode(node.children[n], fn)
	}
}
