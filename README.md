# 报名快照与完整事务增量的本地入湖

这是一个处理本地数据交换的仓湖项目。示例使用虚构机构和匿名合成记录，不连接业务平台。

## 数据契约

快照入口接收完整的一致性全量，其cut表示全量已经包含的源端日志位置。事务入口接收已完整组装的一个事务，position为连续整数，changes按源事务内顺序执行。报名键是enrollment_id，active才计入地区人数。单次快照导入会替换当前副本，独立快照导入不能与另一个接入过程交错。公开代次保存位点、明细文件与汇总。

不停机交接另走连续入口：先登记带全部分片标识和摘要的快照清单，分片经校验暂存、齐全后原子切换；事务拆片独立到达，切点以内的已闭合事务被快照取代不重放，切点以后按position连续应用，缺片或日志缺口期间下游保持上一份完整事实。`Lake.ingestion_status()`可查当前可见位点与所有等待中的输入，详见`docs/data-contract.md`。

## 代码职责

- `enrolllake/intake.py`：完整快照、快照清单/分片、完整事务与事务片的数据契约。
- `enrolllake/replica.py`：报名副本与连续源位点（离线整体入口）。
- `enrolllake/handoff.py`：分片快照与事务片的不停机连续交接、切点推进与接入状态。
- `enrolllake/lake.py`：列式事实和地区汇总代次，以及交接入口的对外封装。
- `enrolllake/storage.py`：本地事务、内容摘要和查询工具。

## 安装与运行

使用 Python 3.11（兼容该系列补丁版本，独立容器镜像以 SHA256 摘要固定）。构建工具和运行依赖均按精确版本登记。先执行 `python3 -m pip install -r requirements-build.txt -r requirements.txt`；依赖仅在准备环境时安装，运行与测试均可断网。

```sh
python3 -m compileall -q enrolllake tests
python3 -m build --wheel --no-isolation
python3 -m unittest discover -s tests -v
python3 -m enrolllake
```

默认示例在独立临时目录执行，结束后删除目录。需要检查持久文件时可执行 `python3 -m enrolllake --root state`。测试自己创建临时目录，不依赖固定端口、外部服务或其他用例顺序。

## 容器

```sh
docker build -t enrollment-bridge:local .
docker run --rm --network none enrollment-bridge:local
docker run --rm --network none enrollment-bridge:local python3 -m enrolllake
```

容器构建负责预装锁定依赖，启动不下载包。示例输出是真实接入、查询和发布的数据，不读取预制输出。测试及示例可分别运行，退出码非零表示失败。

## 交换口径

进一步的字段含义、范围及兼容边界见 `docs/data-contract.md`。
