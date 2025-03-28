# 开发者须知

---
## 自动导包配置

由于 PyCharm 的导包机制可能存在不合理之处，在 `alt` + `enter` 快捷键下，容易出现导包的路径错误，需手动执行以下操作：

**设置 `src` 为资源目录**：
   - 右键点击 `src` 文件夹。
   - 选择 `Mark Directory as` -> `Resources Root`。
   - 重启 PyCharm 以生效。
---
## 如何在django admin的页面中显示一个model？
   - 注册一个model（参考`server\app\models.py`）
   - 在admin后台显示model（参考`server\app\admin.py`）
   - 其他高阶用法持续更新中。。。

---
## 在哪写数据库增删改查？

在 `omniadvisor\repository.py`中，组织django中注册的model，实现增删改查功能