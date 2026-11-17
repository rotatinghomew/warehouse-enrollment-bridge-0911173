# 报名平台交换约定

源端一致性快照由snapshot_id、cut、全部分片标识和各片内容摘要构成；一片可含零行，分片集合由源端清单声明，接收方不能根据超时自行宣布齐全。快照是报名表在cut提交完成后的状态。分片是传输拆分，不改变报名键或快照切点。

源端position是事务提交序号，按1递增，一个事务可能包含多行并被拆成多个传输片。transaction_id与position绑定，事务片序号、片数和最终摘要可用于判断闭合；一个事务的行操作顺序由源端给定，不由文件到达顺序决定。文件中可能重复包含已经接收的事务片。传输文件标识与业务事务标识不同。

报名状态active与cancelled都保留在事实中，只有active计入地区报名人数，delete表示源端不再保留该报名键。快照切点以内的变化已体现在快照中。现有完整snapshot和完整transaction入口分别适用于离线替换与已闭合的连续日志，未约定两种入口可并发直接修改同一副本。

## 连续交接（不停机接入）

快照通道与事务通道独立传送，接收方以两个连续入口把它们并入同一副本，过程中已发布代次始终保持上一份完整事实。

### 快照清单与分片

- 先提交快照清单（snapshot_id、cut、shards；每个shards元素含shard_id与checksum）。清单本身按规范JSON的SHA256留痕；同一snapshot_id重传相同清单幂等，内容不同即冲突；上一快照交接未闭合时不接受新清单。
- 分片文档含snapshot_id、shard_id、rows与checksum。checksum约定为去掉checksum字段后整个文档的规范SHA256；分片必须在清单内、摘要与清单一致才暂存，校验不通过的分片不落任何进度，可原样补传。
- 同报名键在一个快照内只允许出现一次，跨分片重复判为快照不一致。重复送达的已暂存分片按shard_id幂等忽略。
- 清单声明的分片全部暂存后，接收方在单个原子事务内替换enrollment副本、登记snapshots并把source_state置为cut；缺片期间不改动可见副本，缺人不会被解释成退订。

### 事务传输片

- 传输片含transaction_id、position、piece_no、piece_count、final_checksum和本片changes。同一事务所有片的final_checksum相同，等于按源顺序组装后整个事务的规范SHA256（与完整transaction入口指纹口径一致）。
- 片按（transaction_id, piece_no）幂等去重，片序号必须覆盖1..piece_count且最终摘要匹配，否则该事务保持未闭合并等待补片；未闭合事务不产生任何可见变化。
- position不大于当前交接cut的已闭合事务标记为superseded并保留留痕，绝不重放；cut之后的事务从cut+1起按position连续应用。中间缺事务或事务缺片时停在最后一个连续位点，缺口补齐后自动续上，不跳位、不重放。
- 同一transaction_id重传不同position或摘要即冲突报错；已应用事务的重复传输片幂等忽略。

### 状态与可观测性

ingestion_status返回当前可见位点visible_position、下一个期望位点next_expected_position、快照交接阶段（含已暂存/缺失分片标识）、未闭合或等待中的事务（含已收片数/总片数）、日志缺口区间以及被快照取代的事务数。所有暂存与留痕持久化在本地目录，进程退出后重开可继续；advance用于补片/补传后显式重试推进。
