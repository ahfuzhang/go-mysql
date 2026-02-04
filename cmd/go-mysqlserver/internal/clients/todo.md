* 1. 传入 []config.Server 数组，构造多个 mysql client 对象
* 2. 使用项目根目录 client 下面的类，而不是 golang 标准库中提供的 mysql 客户端
* 3. main.go 文件中的 `conn, err := srv.NewConn(c, *user, *passwd, &handler.MyHandler{})` 这样进行修改:
  - 当 host/port/user/pass 与 config.yaml 中一致时，建立对应的 client 对象，进一步实现为一个 mysql 的代理：把客户端的请求，转发到后端，并且把后端的请求再传回给客户端。
  - 当没有匹配的主机时，启动测试主机：user=root, password=123456，然后这个测试主机的请求使用 MyHandle 来处理。
* 4. mysql 的代码服务器，把请求和响应输出到终端，便于查看效果。
