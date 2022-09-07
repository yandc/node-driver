# 探测 & 重试

## 节点探测 & 排序

节点需要暴漏探测相关接口：

``` go
type Node interface {
	// Detect run.
	Detect() error

	// URL returns url of node.
	URL() string
}
```

然后调用探测器的 `DetectOnce` 会:

1. 依次调用 `Node.Detect()` 记录时长和是否有错误；
2. 根据探测接口的耗时和是否范围错误进行排序，形成一个环状结构；
3. 将当前 `Node` 指向第一个（最快的那个）。


## 基于环状的重试机制

先看一下 `WithRetry` 的接口:

``` go
	WithRetry(func(node Node) error) error
```

当完成探测排序后，以下为 `WithRetry` 的逻辑：

1. `Pick()` 选当前 `Node` 传递给回调函数；
   + 成功则返回结果
   + 失败进入 `2`
2. `Failover` 将 `Node` 切换为下一个，如果还有节点没尝试则进入 `1`，否则返回最后一个节点返回的错误。
