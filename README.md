# node-driver

> Chain node driver.


## 安装准备

我们需要一些额外的配置才能在项目中正确使用当前包，请参考如下步骤。

1. 配置 Git 通过 SSH Clone HTTPS 的仓库：

    ``` shell
    git config --global url."ssh://git@gitlab.bixin.com:8222/".insteadOf "https://gitlab.bixin.com/"
    ```
2. 设置 `gitlab.bixin.com` 为私有仓库

    ```shell
    go env -w GOPRIVATE=gitlab.bixin.com
    ```


## 安装

1. 在代码中引用

    ```go
    import "gitlab.bixin.com/mili/node-driver/detector"
    ```

2. 执行 `go mod tidy`

    ```shell
    go mod tidy
    ```


## 构建版本注入

1. 生成构建版本

    ```shell
    $ BUILD_VERSION=$(git describe --always)
    ```

2. 将构建版本通过 `ldflags` 在编译期注入到变量中

    ```shell
    go build -ldflags "-X \"gitlab.bixin.com/milli/node-driver.BuildVersion=$BUILD_VERSION\""
    ```

3. 然后可以在代码中引用并在运行时反射出当前构建版本

    ```go
    import "gitlab.bixin.com/milli/node-driver"

    func main() {
        println(nodedriver.BuildVersion)
    }
    ```
