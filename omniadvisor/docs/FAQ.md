# FAQ 文档

## 开发者需要注意啥？

由于 PyCharm 的导包机制可能存在不合理之处，需手动执行以下操作：

**设置 `src` 为资源目录**：
   - 右键点击 `src` 文件夹。
   - 选择 `Mark Directory as` -> `Resources Root`。
   - 重启 PyCharm 以生效。

---

## 如何拉起单元测试？

在 `src/omniadvisor` 目录下，按照以下步骤运行单元测试：

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
- **多线程运行测试**：
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

## 如何进入 Django 后台管理页面？

在 `src/django_server` 目录下，按照以下步骤操作：

### 数据库迁移
```bash
python manage.py makemigrations
python manage.py migrate
```

### 创建超级用户
```bash
python manage.py createsuperuser
```
按照提示输入用户名、邮箱和密码。

### 启动开发服务器
```bash
python manage.py runserver
```

### 登录管理页面
访问以下地址以登录 Django 后台管理页面：
[http://127.0.0.1:8000/admin](http://127.0.0.1:8000/admin)
