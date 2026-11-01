---
name: qq-member-avatar
description: "Read a QQ group member's public avatar image from their QQ number."
---

# QQ 群成员头像

根据群消息中的 QQ 号构造头像地址：`https://q1.qlogo.cn/g?b=qq&nk=<QQ号>&s=100`。将 `<QQ号>` 替换为目标成员的 QQ 号，然后调用 `web_fetch` 读取该地址；头像由公开地址提供，无需 NapCat 动作或鉴权。

`web_fetch` 会把图像作为图片内容返回。若当前工具列表中没有 `web_fetch`，告知用户当前无法读取头像。
