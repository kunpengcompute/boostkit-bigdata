# FAQ 文档

---

## 进入Django 后台管理页面

在`src`目录下，按照以下步骤操作

### 创建超级用户（若未曾创建）

```bash
python init.py createsuperuser
```

按照提示输入用户名、邮箱和密码

### 启动开发服务

```bash
python init.py runserver
```

### 登录管理页面

访问以下地址以登录 Django 后台管理页面：
[http://127.0.0.1:8000/admin](http://127.0.0.1:8000/admin)


---
## 拉单元测试

在 `omniadvisor` 根目录下，按照以下步骤运行单元测试：

### 安装测试环境依赖

```bash
poetry install --with test
```

该命令用于安装测试环境所需的依赖。

### 激活虚拟环境

```bash
poetry shell
```

### 运行单元测试

- **多线程运行测试（须安装 pytest-xdist）**：
  ```bash
  python -m pytest -n auto
  ```
- **单线程运行测试**：
  ```bash
  python -m pytest
  ```

### 退出虚拟环境

```bash
exit
```

---

## 自动导包配置

由于 PyCharm 的导包机制可能存在不合理之处，在 `alt` + `enter` 快捷键下，容易出现导包的路径错误，需手动执行以下操作：

**设置 `src` 为资源目录**：
   - 右键点击 `src` 文件夹。
   - 选择 `Mark Directory as` -> `Resources Root`。
   - 重启 PyCharm 以生效。

---
## 在django admin的页面中显示一个model
   - 注册一个model（参考`server\app\models.py`）
   - 在admin后台显示model（参考`server\app\admin.py`）
   - 其他高阶用法持续更新中。。。

---
### 数据库迁移
```bash
python init.py makemigrations
python init.py migrate
```

---


