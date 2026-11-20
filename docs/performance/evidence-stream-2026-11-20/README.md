# SSE 容量证据

正式 Gateway 的完整 HTTP、外部 ACK、账务审计和复算脚本在 [public-final.tar.gz](public-final.tar.gz)。解压后执行 `python3 recount.py` 可重算三轮的逐请求交集、窗口吞吐和联合 P99。原始日志中的预热身份单独保留。

四组独立原型在此保留源包、结果摘要和校验清单。较大的逐请求实验记录放在当前工作区的 [.local/capacity-evidence/stream-2026-11-20](../../../.local/capacity-evidence/stream-2026-11-20)，该目录保存每组的完整文件集。在对应组内执行 `shasum -a 256 -c SHA256SUMS` 可校验全部原始文件。三个连续分钟的压缩记录分成四卷，重组步骤在[连续测量说明](prototype-steady/RESULTS.zh.md)。仓库副本可使用各组源包内的命令重新产生记录。
